// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

type Session struct {
	// 与客户端的链接
	Conn *redis.Conn

	// 请求数量
	Ops int64

	// 上次创建时间
	CreateUnix int64
	// 上次请求时间
	LastOpUnix int64

	// 客户端当前select的database
	database int32

	quit bool
	exit sync.Once

	// 统计数据
	stats struct {
		opmap map[string]*opStats
		total atomic2.Int64
		fails atomic2.Int64
		flush struct {
			n    uint
			nano int64
		}
	}
	start sync.Once

	broken atomic2.Bool
	config *Config

	authorized bool
}

func (s *Session) String() string {
	o := &struct {
		Ops        int64  `json:"ops"`
		CreateUnix int64  `json:"create"`
		LastOpUnix int64  `json:"lastop,omitempty"`
		RemoteAddr string `json:"remote"`
	}{
		s.Ops, s.CreateUnix, s.LastOpUnix,
		s.Conn.RemoteAddr(),
	}
	b, _ := json.Marshal(o)
	return string(b)
}


/*
每接到一个redis请求，就创建一个独立的session进行处理（默认的每个session的tcp连接过期时间为75秒，也就是每个请求最多处理75秒）。
这里的第一个参数是net.Conn，Conn是一个通用的面向流的网络连接，多个goroutines可以同时调用Conn的方法
这里的net.Conn就是我们之前Proxy的lproxy这个Listener监听到的19000请求到来的时候返回的net.Conn
*/
func NewSession(sock net.Conn, config *Config) *Session {
	// 创建redis连接
	c := redis.NewConn(sock,
		config.SessionRecvBufsize.AsInt(),
		config.SessionSendBufsize.AsInt(),
	)
	c.ReaderTimeout = config.SessionRecvTimeout.Duration()
	c.WriterTimeout = config.SessionSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.SessionKeepAlivePeriod.Duration())

	s := &Session{
		Conn: c, config: config,
		CreateUnix: time.Now().Unix(),
	}
	s.stats.opmap = make(map[string]*opStats, 16)
	log.Infof("session [%p] create: %s", s, s)
	return s
}

func (s *Session) CloseReaderWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	return s.Conn.CloseReader()
}

func (s *Session) CloseWithError(err error) error {
	s.exit.Do(func() {
		if err != nil {
			log.Infof("session [%p] closed: %s, error: %s", s, s, err)
		} else {
			log.Infof("session [%p] closed: %s, quit", s, s)
		}
	})
	s.broken.Set(true)
	return s.Conn.Close()
}

var (
	ErrRouterNotOnline          = errors.New("router is not online")
	ErrTooManySessions          = errors.New("too many sessions")
	ErrTooManyPipelinedRequests = errors.New("too many pipelined requests")
)

var RespOK = redis.NewString([]byte("OK"))

func (s *Session) Start(d *Router) {
	// 只会执行一次
	s.start.Do(func() {
		// 默认为1000
		if int(incrSessions()) > s.config.ProxyMaxClients {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR max number of clients reached"), true)
				s.CloseWithError(ErrTooManySessions)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		// Router是否在线
		if !d.isOnline() {
			go func() {
				s.Conn.Encode(redis.NewErrorf("ERR router is not online"), true)
				s.CloseWithError(ErrRouterNotOnline)
				s.incrOpFails(nil, nil)
				s.flushOpStats(true)
			}()
			decrSessions()
			return
		}

		// 给RequestChan的buff中赋1024位的数组，并返回一个RequestChan
		// serveProxy 线程创建, 后面loopWriter和loopReader线程使用
		tasks := NewRequestChanBuffer(1024)

		go func() {
			// 从后端redis server的回复中拿到回复
			// 负责合并请求结果 然后返回给客户端
			s.loopWriter(tasks)
			// alive session减一
			decrSessions()
		}()

		go func() {
			// 接收客户端(连接proxy真正的客户端用户)的请求
			// 负责读取和分发请求到后端
			s.loopReader(tasks, d)
			// 所有请求取完或者proxy退出之后，上面的方法就会结束，关闭tasks这个requestChan
			tasks.Close()
		}()
	})
}


//loopReader读取和分发请求到后端，关键代码就是handleRequest函数，传入的两个参数分别是d *Router和r := &Request{}，也就是把结果存到task里面， 后面loopWriter会用到
func (s *Session) loopReader(tasks *RequestChan, d *Router) (err error) {
	defer func() {
		s.CloseReaderWithError(err)
	}()

	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)

	// 循环处理前端请求
	// session只要没有退出，就一直从conn中取请求，直到请求取完就return，然后会关闭tasks这个requestChan
	// 生产线程为本线程, 在处理完客户端请求后将rsp放入chan中, write线程进行消费处理
	for !s.quit {
		//从与前端的redis连接中取出请求参数(from socket)
		multi, err := s.Conn.DecodeMultiBulk()
		if err != nil {
			return err
		}
		if len(multi) == 0 {
			continue
		}
		s.incrOpTotal()

		//检测requestChan的data是否超过配置的每个pipeline最大请求长度，默认为10000
		if tasks.Buffered() > maxPipelineLen {
			return s.incrOpFails(nil, ErrTooManyPipelinedRequests)
		}

		start := time.Now()
		s.LastOpUnix = start.Unix()
		s.Ops++

		// 构造出request结构
		// 这个request贯穿整个流程..
		r := &Request{}

		//这个Multi非常重要，请求的参数就在里面，是一个[]*redis.Resp切片
		r.Multi = multi

		//WaitGroup的作用是，阻塞主线程的执行，一直等到所有的goroutine执行完成。
		//每创建一个goroutine 就把任务队列中任务的数量+1，任务完成，将任务队列中的任务数量-1。有点类似于java里面的CountDownLatch
		//这个Batch用于检测redis请求是否完成（完成的标志是BackendConn调用了setResponse 后端链接中调用的)
		r.Batch = &sync.WaitGroup{}
		r.Database = s.database
		r.UnixNano = start.UnixNano()

		// 处理实际请求->dispatch->forward->backConn.push_back(input)
		if err := s.handleRequest(r, d); err != nil {
			r.Resp = redis.NewErrorf("ERR handle request, %s", err)
			tasks.PushBack(r)
			if breakOnFailure {
				return err
			}
		} else {
			// 此时已经处理完毕，将resp设置为request的一个参数, 不一定处理完成哦
			// 将request添加到当前Session中之前创建的RequestChan中
			// loopWriter后面再遍历RequestChan取出所有请求及结果
			tasks.PushBack(r)
		}
	}
	return nil
}


// 合并请求的处理结果并返回给客户端
// 请求结果由BackendConn处理好之后，放在Request这个struct的*redis.Resp属性中，这里只需要把结果取出
// 可以看到，codis将请求与结果关联起来的方式，就是把结果当做request的一个属性
func (s *Session) loopWriter(tasks *RequestChan) (err error) {
	defer func() {
		s.CloseWithError(err)
		tasks.PopFrontAllVoid(func(r *Request) {
			s.incrOpFails(r, nil)
		})
		s.flushOpStats(true)
	}()

	var (
		breakOnFailure = s.config.SessionBreakOnFailure
		maxPipelineLen = s.config.SessionMaxPipeline
	)

	p := s.Conn.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = maxPipelineLen / 2

	//前面我们在tasks.PushBack(r)中，将请求放入了data []*Request切片，现在就是取出最早的请求及其处理结果
	//如果当前session的requestChan为空，就调用cond.wait让goroutine等待，直到调用pushback又放入请求为止
	// TODO: 所以这里启新的协程了吗? 没有 每个客户端连接会在自己的线程中read/write
	return tasks.PopFrontAll(func(r *Request) error {
		// 拿取后端处理的结果, 如果还没处理完, 会将自己阻塞起, 本身是在自己连接的线程里...
		resp, err := s.handleResponse(r)
		if err != nil {
			resp = redis.NewErrorf("ERR handle response, %s", err)
			if breakOnFailure {
				s.Conn.Encode(resp, true)
				return s.incrOpFails(r, err)
			}
		}
		if err := p.Encode(resp); err != nil {
			return s.incrOpFails(r, err)
		}
		fflush := tasks.IsEmpty()
		if err := p.Flush(fflush); err != nil {
			return s.incrOpFails(r, err)
		} else {
			s.incrOpStats(r, resp.Type)
		}
		if fflush {
			s.flushOpStats(false)
		}
		return nil
	})
}


func (s *Session) handleResponse(r *Request) (*redis.Resp, error) {
	// goroutine都会一直阻塞到所有请求处理完之后，前面我们已经看到，向RequestChan中添加一个请求的时候 request.Batch++
	// 后面BackendConn调用setResponse，也就是完成处理的时候，又会调用Done方法减一
	// 如果所有的请求处理完之后 这里wait会被唤醒
	r.Batch.Wait()

	// 处理完之后...

	//如果是单个的请求，例如SET，这里就为空了
	if r.Coalesce != nil {
		//如果是MSET这种请求，就需要调用之前自定义的Coalesce方法，将请求合并之后再返回
		if err := r.Coalesce(); err != nil {
			return nil, err
		}
	}
	if err := r.Err; err != nil {
		return nil, err
	} else if r.Resp == nil {
		return nil, ErrRespIsRequired
	}
	return r.Resp, nil
}

// 实际处理请求的方法 同步方法吗??
// 异步的, 只负责将请求写入后端bc.input中, bc.loopWriter会被唤醒读出请求
func (s *Session) handleRequest(r *Request, d *Router) error {
	//解析请求 opstr取决于具体的命令 比如说"SET"
	opstr, flag, err := getOpInfo(r.Multi)
	if err != nil {
		return err
	}
	r.OpStr = opstr
	r.OpFlag = flag
	r.Broken = &s.broken

	//有些命令不支持 直接会返回错误
	if flag.IsNotAllowed() {
		return fmt.Errorf("command '%s' is not allowed", opstr)
	}

	switch opstr {
	case "QUIT":
		return s.handleQuit(r)
	case "AUTH":
		return s.handleAuth(r)
	}

	if !s.authorized {
		if s.config.SessionAuth != "" {
			r.Resp = redis.NewErrorf("NOAUTH Authentication required")
			return nil
		}
		s.authorized = true
	}

	//除了SELECT和default之外 都多传入了一个参数 也就是Router
	switch opstr {
	case "SELECT":
		return s.handleSelect(r)
	case "PING":
		return s.handleRequestPing(r, d)
	case "INFO":
		return s.handleRequestInfo(r, d)
	case "MGET":
		return s.handleRequestMGet(r, d)
	case "MSET":
		return s.handleRequestMSet(r, d)
	case "DEL":
		return s.handleRequestDel(r, d)
	case "EXISTS":
		return s.handleRequestExists(r, d)
	case "SLOTSINFO":
		return s.handleRequestSlotsInfo(r, d)
	case "SLOTSSCAN":
		return s.handleRequestSlotsScan(r, d)
	case "SLOTSMAPPING":
		return s.handleRequestSlotsMapping(r, d)
	default:
		return d.dispatch(r)
	}
}

func (s *Session) handleQuit(r *Request) error {
	s.quit = true
	r.Resp = RespOK
	return nil
}

func (s *Session) handleAuth(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'AUTH' command")
		return nil
	}
	switch {
	case s.config.SessionAuth == "":
		r.Resp = redis.NewErrorf("ERR Client sent AUTH, but no password is set")
	case s.config.SessionAuth != string(r.Multi[1].Value):
		s.authorized = false
		r.Resp = redis.NewErrorf("ERR invalid password")
	default:
		s.authorized = true
		r.Resp = RespOK
	}
	return nil
}

func (s *Session) handleSelect(r *Request) error {
	if len(r.Multi) != 2 {
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SELECT' command")
		return nil
	}
	//将字符串转换为十进制整数，从请求参数中取出是对哪个db进行操作
	switch db, err := strconv.Atoi(string(r.Multi[1].Value)); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR invalid DB index")
	case db < 0 || db >= int(s.config.BackendNumberDatabases):
		r.Resp = redis.NewErrorf("ERR invalid DB index, only accept DB [0,%d)", s.config.BackendNumberDatabases)
	default:
		r.Resp = RespOK
		s.database = int32(db)
	}
	return nil
}

func (s *Session) handleRequestPing(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % MaxSlotNum
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0:
		slot := uint32(time.Now().Nanosecond()) % MaxSlotNum
		return d.dispatchSlot(r, int(slot))
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestMGet(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MGET' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		// 这里也是复用通用逻辑 mget10个key r.Batch就会+10
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var array = make([]*redis.Resp, len(sub))
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsArray() && len(resp.Array) == 1:
				array[i] = resp.Array[0]
			default:
				return fmt.Errorf("bad mget resp: %s array.len = %d", resp.Type, len(resp.Array))
			}
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	return nil
}


//调用Router的dispatch方法，将请求分发给相应的槽进行处理，
//如果调用Mset，一次设置多个值的话，就还要多做一步，通过Coalesce来合并请求结果
func (s *Session) handleRequestMSet(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks == 0 || nblks%2 != 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'MSET' command")
		return nil
	case nblks == 2:
		return d.dispatch(r)
	}
	//将一个Mset请求拆分成多个子set请求，分别dispatch
	var sub = r.MakeSubRequest(nblks / 2)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i*2+1],
			r.Multi[i*2+2],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsString():
				r.Resp = resp
			default:
				return fmt.Errorf("bad mset resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		return nil
	}
	return nil
}

func (s *Session) handleRequestDel(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'DEL' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				n += int(resp.Value[0] - '0')
			default:
				return fmt.Errorf("bad del resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestExists(r *Request, d *Router) error {
	var nkeys = len(r.Multi) - 1
	switch {
	case nkeys == 0:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'EXISTS' command")
		return nil
	case nkeys == 1:
		return d.dispatch(r)
	}
	var sub = r.MakeSubRequest(nkeys)
	for i := range sub {
		sub[i].Multi = []*redis.Resp{
			r.Multi[0],
			r.Multi[i+1],
		}
		if err := d.dispatch(&sub[i]); err != nil {
			return err
		}
	}
	r.Coalesce = func() error {
		var n int
		for i := range sub {
			if err := sub[i].Err; err != nil {
				return err
			}
			switch resp := sub[i].Resp; {
			case resp == nil:
				return ErrRespIsRequired
			case resp.IsInt() && len(resp.Value) == 1:
				if resp.Value[0] != '0' {
					n++
				}
			default:
				return fmt.Errorf("bad exists resp: %s value.len = %d", resp.Type, len(resp.Value))
			}
		}
		r.Resp = redis.NewInt(strconv.AppendInt(nil, int64(n), 10))
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsInfo(r *Request, d *Router) error {
	var addr string
	var nblks = len(r.Multi) - 1
	switch {
	case nblks != 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSINFO' command")
		return nil
	default:
		addr = string(r.Multi[1].Value)
		copy(r.Multi[1:], r.Multi[2:])
		r.Multi = r.Multi[:nblks]
	}
	if !d.dispatchAddr(r, addr) {
		r.Resp = redis.NewErrorf("ERR backend server '%s' not found", addr)
		return nil
	}
	return nil
}

func (s *Session) handleRequestSlotsScan(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks <= 1:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSSCAN' command")
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= MaxSlotNum:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		return d.dispatchSlot(r, int(slot))
	}
}

func (s *Session) handleRequestSlotsMapping(r *Request, d *Router) error {
	var nblks = len(r.Multi) - 1
	switch {
	case nblks >= 2:
		r.Resp = redis.NewErrorf("ERR wrong number of arguments for 'SLOTSMAPPING' command")
		return nil
	}
	marshalToResp := func(m *models.Slot) *redis.Resp {
		if m == nil {
			return redis.NewArray(nil)
		}
		var replicaGroups []*redis.Resp
		for i := range m.ReplicaGroups {
			var group []*redis.Resp
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, redis.NewString([]byte(addr)))
			}
			replicaGroups = append(replicaGroups, redis.NewArray(group))
		}
		return redis.NewArray([]*redis.Resp{
			redis.NewString([]byte(strconv.Itoa(m.Id))),
			redis.NewString([]byte(m.BackendAddr)),
			redis.NewString([]byte(m.MigrateFrom)),
			redis.NewArray(replicaGroups),
		})
	}
	if nblks == 0 {
		var array = make([]*redis.Resp, MaxSlotNum)
		for i, m := range d.GetSlots() {
			array[i] = marshalToResp(m)
		}
		r.Resp = redis.NewArray(array)
		return nil
	}
	switch slot, err := redis.Btoi64(r.Multi[1].Value); {
	case err != nil:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, %s", r.Multi[1].Value, err)
		return nil
	case slot < 0 || slot >= MaxSlotNum:
		r.Resp = redis.NewErrorf("ERR parse slotnum '%s' failed, out of range", r.Multi[1].Value)
		return nil
	default:
		r.Resp = marshalToResp(d.GetSlot(int(slot)))
		return nil
	}
}

func (s *Session) incrOpTotal() {
	s.stats.total.Incr()
}

func (s *Session) getOpStats(opstr string) *opStats {
	e := s.stats.opmap[opstr]
	if e == nil {
		e = &opStats{opstr: opstr}
		s.stats.opmap[opstr] = e
	}
	return e
}

func (s *Session) incrOpStats(r *Request, t redis.RespType) {
	e := s.getOpStats(r.OpStr)
	e.calls.Incr()
	e.nsecs.Add(time.Now().UnixNano() - r.UnixNano)
	switch t {
	case redis.TypeError:
		e.redis.errors.Incr()
	}
}

func (s *Session) incrOpFails(r *Request, err error) error {
	if r != nil {
		e := s.getOpStats(r.OpStr)
		e.fails.Incr()
	} else {
		s.stats.fails.Incr()
	}
	return err
}

func (s *Session) flushOpStats(force bool) {
	var nano = time.Now().UnixNano()
	if !force {
		const period = int64(time.Millisecond) * 100
		if d := nano - s.stats.flush.nano; d < period {
			return
		}
	}
	s.stats.flush.nano = nano

	incrOpTotal(s.stats.total.Swap(0))
	incrOpFails(s.stats.fails.Swap(0))
	for _, e := range s.stats.opmap {
		if e.calls.Int64() != 0 || e.fails.Int64() != 0 {
			incrOpStats(e)
		}
	}
	s.stats.flush.n++

	if len(s.stats.opmap) <= 32 {
		return
	}
	if (s.stats.flush.n % 16384) == 0 {
		s.stats.opmap = make(map[string]*opStats, 32)
	}
}
