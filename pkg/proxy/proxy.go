// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/unsafe2"
)


/*models.Proxy的详细信息会挂载在zk目录”/codis3/codis-wujiang/proxy/proxy-token”下

{
    "id": 1,
    "token": "0317b8f67921f8c7a2d19d372cc9511b",
    "start_time": "2017-07-28 14:44:36.462306337 +0800 CST",
    "admin_addr": "*.*.*.*:11080",
    "proto_type": "tcp4",
    "proxy_addr": "*.*.*.*:19000",
    "jodis_path": "/jodis/codis-dev/proxy-0317b8f67921f8c7a2d19d372cc9511b",
    "product_name": "codis-dev",
    "pid": 45092,
    "pwd": "/app/codis",
    "sys": "Linux cnsz22vla888.novalocal 2.6.32-504.el6.x86_64 #1 SMP Wed Oct 15 04:27:16 UTC 2014 x86_64 x86_64 x86_64 GNU/Linux",
    "hostname": "cnsz22vla888.novalocal",
    "datacenter": ""
}
 */

type Proxy struct {
	mu sync.Mutex

	xauth string

	//proxy创建成功之后
	//在zookeeper的/codis3/codis-dev/proxy/proxy-token中展示的信息
	model *models.Proxy

	//接收退出信息的channel
	exit struct {
		C chan struct{}
	}
	online bool
	closed bool

	config *Config

	// 与后端group通信
	//Router中存储了集群中所有sharedBackendConnPool和slot，用于将redis请求转发给相应的slot进行处理
	router *Router
	ignore []byte

	//监听proxy的19000端口的Listener，也就是proxy实际工作的端口
	lproxy net.Listener

	//监听proxy_admin的11080端口的Listener，也就是codis集群和proxy进行交互的端口
	ladmin net.Listener

	ha struct {
		//上帝视角sentinel，并不是真正的物理服务器
		//s := &Sentinel{Product: product, Auth: auth}
		//s.Context, s.Cancel = context.WithCancel(context.Background())
		monitor *redis.Sentinel
		//int为groupId，标示每个group的主服务器
		masters map[int]string
		//当前集群中的所有物理sentinel
		servers []string
	}

	//java客户端Jodis与codis集群交互，就是通过下面的struct
	//里面存储了zkClient以及"/jodis/codis-dev/proxy-token"这个路径
	jodis *Jodis
}

var ErrClosedProxy = errors.New("use of closed proxy")

func New(config *Config) (*Proxy, error) {
	//config的参数校验
	if err := config.Validate(); err != nil {
		return nil, errors.Trace(err)
	}
	if err := models.ValidateProduct(config.ProductName); err != nil {
		return nil, errors.Trace(err)
	}

	//新建一个Proxy，后面就是它填充属性
	s := &Proxy{}
	s.config = config
	s.exit.C = make(chan struct{})

	// 创建route 以及route内各自slot 以及slot对应的后端链接
	//通过config new一个Router，这一步只是初始化了Router中的两个sharedBackendConnPool的结构，
	//也就是map[string]*sharedBackendConn
	s.router = NewRouter(config)
	s.ignore = make([]byte, config.ProxyHeapPlaceholder.Int64())

	s.model = &models.Proxy{
		StartTime: time.Now().String(),
	}
	s.model.ProductName = config.ProductName
	s.model.DataCenter = config.ProxyDataCenter
	//获得当前进程的进程号
	s.model.Pid = os.Getpid()
	//返回路径的字符串
	s.model.Pwd, _ = os.Getwd()
	//获取当前操作系统信息和主机名
	if b, err := exec.Command("uname", "-a").Output(); err != nil {
		log.WarnErrorf(err, "run command uname failed")
	} else {
		s.model.Sys = strings.TrimSpace(string(b))
	}
	s.model.Hostname = utils.Hostname

	// 开始监听 接收客户端连接请求
	//将config中的参数设置到models.proxy里，主要设置的参数是admin_addr,proxy_addr,设置之后调用net.Listen进行监听，成功的话将Listener存入proxy，生成Token和xauth，以及proxy.Jodis。这里也设置了zk最终的树形路径
	//如果配置文件中的jodis_compatible设置的是false，就采用codis3的zk路径，jodis/codis-demo/proxy-token。如果兼容这里设置的true，就还采用codis2时期的/zk/codis/db_productName
	if err := s.setup(config); err != nil {
		s.Close()
		return nil, err
	}

	log.Warnf("[%p] create new proxy:\n%s", s, s.model.Encode())

	unsafe2.SetMaxOffheapBytes(config.ProxyMaxOffheapBytes.Int64())

	//新建一个路由表，对发送到11080端口的请求做处理
	go s.serveAdmin()
	//启动goroutine来监听发送到19000端口的redis请求
	go s.serveProxy()

	s.startMetricsJson()
	s.startMetricsInfluxdb()
	s.startMetricsStatsd()

	return s, nil
}

func (s *Proxy) setup(config *Config) error {
	proto := config.ProtoType
	if l, err := net.Listen(proto, config.ProxyAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.lproxy = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostProxy)
		if err != nil {
			return err
		}
		s.model.ProtoType = proto
		s.model.ProxyAddr = x
	}

	proto = "tcp"
	if l, err := net.Listen(proto, config.AdminAddr); err != nil {
		return errors.Trace(err)
	} else {
		s.ladmin = l

		x, err := utils.ReplaceUnspecifiedIP(proto, l.Addr().String(), config.HostAdmin)
		if err != nil {
			return err
		}
		s.model.AdminAddr = x
	}

	s.model.Token = rpc.NewToken(
		config.ProductName,
		s.lproxy.Addr().String(),
		s.ladmin.Addr().String(),
	)
	s.xauth = rpc.NewXAuth(
		config.ProductName,
		config.ProductAuth,
		s.model.Token,
	)

	if config.JodisAddr != "" {
		c, err := models.NewClient(config.JodisName, config.JodisAddr, config.JodisAuth, config.JodisTimeout.Duration())
		if err != nil {
			return err
		}
		if config.JodisCompatible {
			s.model.JodisPath = filepath.Join("/zk/codis", fmt.Sprintf("db_%s", config.ProductName), "proxy", s.model.Token)
		} else {
			s.model.JodisPath = models.JodisPath(config.ProductName, s.model.Token)
		}
		s.jodis = NewJodis(c, s.model)
	}

	return nil
}

// 将Proxy自身，和它的router和jodis设为上线
// 在zk中创建临时节点/jodis/codis-productName/proxy-token，并监听该节点的变化
func (s *Proxy) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	if s.online {
		return nil
	}
	s.online = true
	//router的online属性设为true
	s.router.Start()
	if s.jodis != nil {
		s.jodis.Start()
	}
	return nil
}

func (s *Proxy) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	close(s.exit.C)

	if s.jodis != nil {
		s.jodis.Close()
	}
	if s.ladmin != nil {
		s.ladmin.Close()
	}
	if s.lproxy != nil {
		s.lproxy.Close()
	}
	if s.router != nil {
		s.router.Close()
	}
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
	}
	return nil
}

func (s *Proxy) XAuth() string {
	return s.xauth
}

func (s *Proxy) Model() *models.Proxy {
	return s.model
}

func (s *Proxy) Config() *Config {
	return s.config
}

func (s *Proxy) IsOnline() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.online && !s.closed
}

func (s *Proxy) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Proxy) HasSwitched() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.HasSwitched()
}

func (s *Proxy) Slots() []*models.Slot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.router.GetSlots()
}

func (s *Proxy) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	return s.router.FillSlot(m)
}

//填充Slot
//这里传过来的slots实际上只有一个，就是整个集群中最小的Action.State对应的Slotmapping
func (s *Proxy) FillSlots(slots []*models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	// 为每个slots创建后端链接和初始化
	for _, m := range slots {
		if err := s.router.FillSlot(m); err != nil {
			return err
		}
	}
	return nil
}

func (s *Proxy) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.masters = masters

	if len(masters) != 0 {
		s.router.SwitchMasters(masters)
	}
	return nil
}

func (s *Proxy) GetSentinels() ([]string, map[int]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, nil
	}
	return s.ha.servers, s.ha.masters
}

func (s *Proxy) SetSentinels(servers []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	s.ha.servers = servers
	log.Warnf("[%p] set sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) RewatchSentinels() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedProxy
	}
	log.Warnf("[%p] rewatch sentinels = %v", s, s.ha.servers)

	s.rewatchSentinels(s.ha.servers)
	return nil
}

func (s *Proxy) rewatchSentinels(servers []string) {
	if s.ha.monitor != nil {
		s.ha.monitor.Cancel()
		s.ha.monitor = nil
		s.ha.masters = nil
	}
	if len(servers) != 0 {
		s.ha.monitor = redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
		s.ha.monitor.LogFunc = log.Warnf
		s.ha.monitor.ErrFunc = log.WarnErrorf
		go func(p *redis.Sentinel) {
			var trigger = make(chan struct{}, 1)
			delayUntil := func(deadline time.Time) {
				for !p.IsCanceled() {
					var d = deadline.Sub(time.Now())
					if d <= 0 {
						return
					}
					time.Sleep(math2.MinDuration(d, time.Second))
				}
			}
			go func() {
				defer close(trigger)
				callback := func() {
					select {
					case trigger <- struct{}{}:
					default:
					}
				}
				for !p.IsCanceled() {
					timeout := time.Minute * 15
					retryAt := time.Now().Add(time.Second * 10)
					if !p.Subscribe(servers, timeout, callback) {
						delayUntil(retryAt)
					} else {
						callback()
					}
				}
			}()
			go func() {
				for range trigger {
					var success int
					for i := 0; i != 10 && !p.IsCanceled() && success != 2; i++ {
						timeout := time.Second * 5
						masters, err := p.Masters(servers, timeout)
						if err != nil {
							log.WarnErrorf(err, "[%p] fetch group masters failed", s)
						} else {
							if !p.IsCanceled() {
								s.SwitchMasters(masters)
							}
							success += 1
						}
						delayUntil(time.Now().Add(time.Second * 5))
					}
				}
			}()
		}(s.ha.monitor)
	}
}

/*
路由表本质上其实就是一个map，key是路径 ==> “/”，value是该路径所对应的处理函数 ==> 在我们这里就是newApiServer(s)出来的处理函数。
codis-proxy启动之后，这一步是启动http服务，当集群配置命令请求向prxoy_admin的11080端口发过来之后，转发做相应处理。
newApiServer方法使用了这里使用了martini框架。有关于martini框架的详细信息，可以参见http://www.oschina.net/p/martini/
*/

func (s *Proxy) serveAdmin() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] admin start service on %s", s, s.ladmin.Addr())

	eh := make(chan error, 1)
	go func(l net.Listener) {
		//新建路由表
		h := http.NewServeMux()
		//这里表示newApiServer用来处理所有"/"路径的请求
		h.Handle("/", newApiServer(s))
		hs := &http.Server{Handler: h}
		//对每个net.Listener的连接，新建goroutine读请求，并调用srv.Handler进行处理
		eh <- hs.Serve(l)
	}(s.ladmin)

	select {
	case <-s.exit.C:
		log.Warnf("[%p] admin shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] admin exit on error", s)
	}
}

func (s *Proxy) serveProxy() {
	if s.IsClosed() {
		return
	}
	defer s.Close()

	log.Warnf("[%p] proxy start service on %s", s, s.lproxy.Addr())

	eh := make(chan error, 1)
	// 等待客户端连接
	go func(l net.Listener) (err error) {
		defer func() {
			eh <- err
		}()
		for {
			// 创建session
			// 启动goroutine来监听redis请求
			c, err := s.acceptConn(l)
			if err != nil {
				return err
			}
			NewSession(c, s.config).Start(s.router)
		}
	}(s.lproxy)

	if d := s.config.BackendPingPeriod.Duration(); d != 0 {
		go s.keepAlive(d)
	}

	select {
	case <-s.exit.C:
		log.Warnf("[%p] proxy shutdown", s)
	case err := <-eh:
		log.ErrorErrorf(err, "[%p] proxy exit on error", s)
	}
}

func (s *Proxy) keepAlive(d time.Duration) {
	var ticker = time.NewTicker(math2.MaxDuration(d, time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-s.exit.C:
			return
		case <-ticker.C:
			s.router.KeepAlive()
		}
	}
}

func (s *Proxy) acceptConn(l net.Listener) (net.Conn, error) {
	var delay = &DelayExp2{
		Min: 10, Max: 500,
		Unit: time.Millisecond,
	}
	for {
		c, err := l.Accept()
		if err != nil {
			if e, ok := err.(net.Error); ok && e.Temporary() {
				log.WarnErrorf(err, "[%p] proxy accept new connection failed", s)
				delay.Sleep()
				continue
			}
		}
		return c, err
	}
}

type Overview struct {
	Version string         `json:"version"`
	Compile string         `json:"compile"`
	Config  *Config        `json:"config,omitempty"`
	Model   *models.Proxy  `json:"model,omitempty"`
	Stats   *Stats         `json:"stats,omitempty"`
	Slots   []*models.Slot `json:"slots,omitempty"`
}

type Stats struct {
	Online bool `json:"online"`
	Closed bool `json:"closed"`

	Sentinels struct {
		Servers  []string          `json:"servers,omitempty"`
		Masters  map[string]string `json:"masters,omitempty"`
		Switched bool              `json:"switched,omitempty"`
	} `json:"sentinels"`

	Ops struct {
		Total int64 `json:"total"`
		Fails int64 `json:"fails"`
		Redis struct {
			Errors int64 `json:"errors"`
		} `json:"redis"`
		QPS int64      `json:"qps"`
		Cmd []*OpStats `json:"cmd,omitempty"`
	} `json:"ops"`

	Sessions struct {
		Total int64 `json:"total"`
		Alive int64 `json:"alive"`
	} `json:"sessions"`

	Rusage struct {
		Now string       `json:"now"`
		CPU float64      `json:"cpu"`
		Mem int64        `json:"mem"`
		Raw *utils.Usage `json:"raw,omitempty"`
	} `json:"rusage"`

	Backend struct {
		PrimaryOnly bool `json:"primary_only"`
	} `json:"backend"`

	Runtime *RuntimeStats `json:"runtime,omitempty"`
}

type RuntimeStats struct {
	General struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Lookups uint64 `json:"lookups"`
		Mallocs uint64 `json:"mallocs"`
		Frees   uint64 `json:"frees"`
	} `json:"general"`

	Heap struct {
		Alloc   uint64 `json:"alloc"`
		Sys     uint64 `json:"sys"`
		Idle    uint64 `json:"idle"`
		Inuse   uint64 `json:"inuse"`
		Objects uint64 `json:"objects"`
	} `json:"heap"`

	GC struct {
		Num          uint32  `json:"num"`
		CPUFraction  float64 `json:"cpu_fraction"`
		TotalPauseMs uint64  `json:"total_pausems"`
	} `json:"gc"`

	NumProcs      int   `json:"num_procs"`
	NumGoroutines int   `json:"num_goroutines"`
	NumCgoCall    int64 `json:"num_cgo_call"`
	MemOffheap    int64 `json:"mem_offheap"`
}

type StatsFlags uint32

func (s StatsFlags) HasBit(m StatsFlags) bool {
	return (s & m) != 0
}

const (
	StatsCmds = StatsFlags(1 << iota)
	StatsSlots
	StatsRuntime

	StatsFull = StatsFlags(^uint32(0))
)

func (s *Proxy) Overview(flags StatsFlags) *Overview {
	o := &Overview{
		Version: utils.Version,
		Compile: utils.Compile,
		Config:  s.Config(),
		Model:   s.Model(),
		Stats:   s.Stats(flags),
	}
	if flags.HasBit(StatsSlots) {
		o.Slots = s.Slots()
	}
	return o
}

func (s *Proxy) Stats(flags StatsFlags) *Stats {
	stats := &Stats{}
	stats.Online = s.IsOnline()
	stats.Closed = s.IsClosed()

	servers, masters := s.GetSentinels()
	if servers != nil {
		stats.Sentinels.Servers = servers
	}
	if masters != nil {
		stats.Sentinels.Masters = make(map[string]string)
		for gid, addr := range masters {
			stats.Sentinels.Masters[strconv.Itoa(gid)] = addr
		}
	}
	stats.Sentinels.Switched = s.HasSwitched()

	stats.Ops.Total = OpTotal()
	stats.Ops.Fails = OpFails()
	stats.Ops.Redis.Errors = OpRedisErrors()
	stats.Ops.QPS = OpQPS()

	if flags.HasBit(StatsCmds) {
		stats.Ops.Cmd = GetOpStatsAll()
	}

	stats.Sessions.Total = SessionsTotal()
	stats.Sessions.Alive = SessionsAlive()

	if u := GetSysUsage(); u != nil {
		stats.Rusage.Now = u.Now.String()
		stats.Rusage.CPU = u.CPU
		stats.Rusage.Mem = u.MemTotal()
		stats.Rusage.Raw = u.Usage
	}

	stats.Backend.PrimaryOnly = s.Config().BackendPrimaryOnly

	if flags.HasBit(StatsRuntime) {
		var r runtime.MemStats
		runtime.ReadMemStats(&r)

		stats.Runtime = &RuntimeStats{}
		stats.Runtime.General.Alloc = r.Alloc
		stats.Runtime.General.Sys = r.Sys
		stats.Runtime.General.Lookups = r.Lookups
		stats.Runtime.General.Mallocs = r.Mallocs
		stats.Runtime.General.Frees = r.Frees
		stats.Runtime.Heap.Alloc = r.HeapAlloc
		stats.Runtime.Heap.Sys = r.HeapSys
		stats.Runtime.Heap.Idle = r.HeapIdle
		stats.Runtime.Heap.Inuse = r.HeapInuse
		stats.Runtime.Heap.Objects = r.HeapObjects
		stats.Runtime.GC.Num = r.NumGC
		stats.Runtime.GC.CPUFraction = r.GCCPUFraction
		stats.Runtime.GC.TotalPauseMs = r.PauseTotalNs / uint64(time.Millisecond)
		stats.Runtime.NumProcs = runtime.GOMAXPROCS(0)
		stats.Runtime.NumGoroutines = runtime.NumGoroutine()
		stats.Runtime.NumCgoCall = runtime.NumCgoCall()
		stats.Runtime.MemOffheap = unsafe2.OffheapBytes()
	}
	return stats
}
