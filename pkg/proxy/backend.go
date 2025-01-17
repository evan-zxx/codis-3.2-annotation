// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/math2"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)

const (
	stateConnected = iota + 1
	stateDataStale
)

type BackendConn struct {
	stop sync.Once
	addr string

	// 前端请求打到后端server的请求队列 session loopReader的请求
	// buffer为1024的channel
	input chan *Request
	retry struct {
		fails int
		delay Delay
	}
	state atomic2.Int64

	closed atomic2.Bool
	config *Config

	//0~15
	database int
}

// 每个backendConn会创建对应的reader和writer
func NewBackendConn(addr string, database int, config *Config) *BackendConn {
	bc := &BackendConn{
		addr: addr, config: config, database: database,
	}
	bc.input = make(chan *Request, 1024)
	bc.retry.delay = &DelayExp2{
		Min: 50, Max: 5000,
		Unit: time.Millisecond,
	}

	go bc.run()

	return bc
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
	bc.closed.Set(true)
}

func (bc *BackendConn) IsConnected() bool {
	return bc.state.Int64() == stateConnected
}


//请求放入BackendConn等待 处理。如果request的sync.WaitGroup不为空，就加一，然后判断加一之后的值，如果加一之后couter为0，那么所有阻塞在counter上的goroutine都会得到释放
//将请求直接存入到BackendConn的chan *Request中，等待后续被取出并进行处理
func (bc *BackendConn) PushBack(r *Request) {

	if r.Batch != nil {
		// 请求处理之后，setResponse的时候，r.Batch会减1，表明请求已经处理完。
		// session的loopWriter里面收集请求结果的时候，会调用wait方法等一次的所有请求处理完成
		// add banth and ret;
		r.Batch.Add(1)
	}

	// 将前端请求写进队列
	// 将session传来的Request写入chan
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}
	switch bc.state.Int64() {
	default:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}
		bc.PushBack(m)

	case stateDataStale:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("INFO")),
		}
		m.Batch = &sync.WaitGroup{}
		bc.PushBack(m)

		keepAliveCallback <- func() {
			m.Batch.Wait()
			var err = func() error {
				if err := m.Err; err != nil {
					return err
				}
				switch resp := m.Resp; {
				case resp == nil:
					return ErrRespIsRequired
				case resp.IsError():
					return fmt.Errorf("bad info resp: %s", resp.Value)
				case resp.IsBulkBytes():
					var info = make(map[string]string)
					for _, line := range strings.Split(string(resp.Value), "\n") {
						kv := strings.SplitN(line, ":", 2)
						if len(kv) != 2 {
							continue
						}
						if key := strings.TrimSpace(kv[0]); key != "" {
							info[key] = strings.TrimSpace(kv[1])
						}
					}
					if info["master_link_status"] == "down" {
						return nil
					}
					if info["loading"] == "1" {
						return nil
					}
					if bc.state.CompareAndSwap(stateDataStale, stateConnected) {
						log.Warnf("backend conn [%p] to %s, db-%d state = Connected (keepalive)",
							bc, bc.addr, bc.database)
					}
					return nil
				default:
					return fmt.Errorf("bad info resp: should be string, but got %s", resp.Type)
				}
			}()
			if err != nil && bc.closed.IsFalse() {
				log.WarnErrorf(err, "backend conn [%p] to %s, db-%d recover from DataStale failed",
					bc, bc.addr, bc.database)
			}
		}
	}
	return true
}

var keepAliveCallback = make(chan func(), 128)

func init() {
	go func() {
		for fn := range keepAliveCallback {
			fn()
		}
	}()
}

// 从后端的codis-server读取回包
func (bc *BackendConn) newBackendReader(round int, config *Config) (*redis.Conn, chan<- *Request, error) {
	// 建立后端codis-server的链接 just for testing select? no
	c, err := redis.DialTimeout(bc.addr, time.Second*5,
		config.BackendRecvBufsize.AsInt(),
		config.BackendSendBufsize.AsInt())
	if err != nil {
		return nil, nil, err
	}
	c.ReaderTimeout = config.BackendRecvTimeout.Duration()
	c.WriterTimeout = config.BackendSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.BackendKeepAlivePeriod.Duration())

	if err := bc.verifyAuth(c, config.ProductAuth); err != nil {
		c.Close()
		return nil, nil, err
	}
	// 选择后端redis的库
	if err := bc.selectDatabase(c, bc.database); err != nil {
		c.Close()
		return nil, nil, err
	}

	// 处理req和rsp都是通过read和write公用chan *Request来实现通信,
	// BackendConn与后端redis  session和client都是这种方式
	tasks := make(chan *Request, config.BackendMaxPipeline)
	// 从redis tcp读数据的代码在哪... 在codec中
	// 读取task中的请求，并将处理结果与之对应关联
	go bc.loopReader(tasks, c, round)

	return c, tasks, nil
}

func (bc *BackendConn) verifyAuth(c *redis.Conn, auth string) error {
	if auth == "" {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(auth)),
	}

	// 做redis鉴权
	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) selectDatabase(c *redis.Conn, database int) error {
	if database == 0 {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("SELECT")),
		redis.NewBulkBytes([]byte(strconv.Itoa(database))),
	}

	// 这里EncodeMultiBulk相当于直接同步执行了redis cli select命令
	// 随后直接Decode拿到redis的回包
	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	// 这里将后端的回复赋值到了r.Resp中
	r.Resp, r.Err = resp, err

	// 如果处理的是mget类的请求, 则将完成+1
	if r.Group != nil {
		r.Group.Done()
	}
	// 其余的请求: 单个请求/使用pipeline都走这个逻辑(其实都是默认使用pipeline, 只是单个请求数量设置为1)
	// 这里已经收到了后端codis-server的返回值 将Batch-1
	// 从session到BackendConn都在共用这个request
	// 如果所有的处理都完成了 则会唤醒backend中的线程进行后续处理
	if r.Batch != nil {
		r.Batch.Done()
	}
	return err
}

var (
	ErrBackendConnReset = errors.New("backend conn reset")
	ErrRequestIsBroken  = errors.New("request is broken")
)

//NewBackendConn的时候启动的goroutine
func (bc *BackendConn) run() {
	log.Warnf("backend conn [%p] to %s, db-%d start service",
		bc, bc.addr, bc.database)
	for round := 0; bc.closed.IsFalse(); round++ {
		log.Warnf("backend conn [%p] to %s, db-%d round-[%d]",
			bc, bc.addr, bc.database, round)
		// ready recv from backend redis
		// 这个loopWriter方法是新建sharedBackendConn的核心方法，里面不止创建了loopWriter，也创建了loopReader
		// LoopWriter负责将客户端的redis请求取出并发送到后端redis
		if err := bc.loopWriter(round); err != nil {
			bc.delayBeforeRetry()
		}
	}
	log.Warnf("backend conn [%p] to %s, db-%d stop and exit",
		bc, bc.addr, bc.database)
}

var (
	errRespMasterDown = []byte("MASTERDOWN")
	errRespLoading    = []byte("LOADING")
)

func (bc *BackendConn) loopReader(tasks <-chan *Request, c *redis.Conn, round int) (err error) {
	// 从连接中取完所有请求并setResponse之后，连接就会关闭
	defer func() {
		c.Close()
		for r := range tasks {
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d reader-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()

	// loopWriter中将客户端请求通过EncodeMultiBulk发送到后端redis后, 会将请求写入此chanel
	// 在reader线程进行遍历tasks，此时的r是之前writer线程写入的task (所有的请求)
	// 每个BackendConn都会有对应的redis.Conn和两个线程reader/writer, 所以这里只需要直接从c中Decode就好
	// 这里的tasks是writeLoop写入的

	// 最开始会先阻塞在tasks <-chan
	// 等write线程push request后进入for循环, 继续阻塞在decode()的socket read()上
	// 等后端codis-server处理完请求将发送回包后, 重新唤醒该线程进行拆包处理
	for r := range tasks {
		// 从redis.Conn中read并解码得到处理结果
		// 阻塞在Decode上, 只有redis发送的回包, 进而继续处理
		resp, err := c.Decode()
		if err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		if resp != nil && resp.IsError() {
			switch {
			case bytes.HasPrefix(resp.Value, errRespMasterDown):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'MASTERDOWN'",
						bc, bc.addr, bc.database)
				}
			case bytes.HasPrefix(resp.Value, errRespLoading):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'LOADING'",
						bc, bc.addr, bc.database)
				}
			}
		}
		// 请求结果设置为请求的属性
		bc.setResponse(r, resp, nil)
	}
	return nil
}

func (bc *BackendConn) delayBeforeRetry() {
	bc.retry.fails += 1
	if bc.retry.fails <= 10 {
		return
	}
	timeout := bc.retry.delay.After()
	for bc.closed.IsFalse() {
		select {
		case <-timeout:
			return
		case r, ok := <-bc.input:
			if !ok {
				return
			}
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
	}
}

// 此为BackendConn的loopWriter直接与后端codis-server通信
// session的loopWriter是与客户端通信, 将处理好的结果返回给客户端, 通过公用一个request
func (bc *BackendConn) loopWriter(round int) (err error) {
	// 如果因为某种原因退出，还有input没来得及处理，就返回错误
	defer func() {
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d writer-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()

	// do recv from backend redis && revc data to bc.input
	// 这个方法内启动了loopReader
	// 返回的tasks串联reader&writer
	c, tasks, err := bc.newBackendReader(round, bc.config)
	if err != nil {
		return err
	}
	defer close(tasks)

	defer bc.state.Set(0)

	bc.state.Set(stateConnected)
	bc.retry.fails = 0
	bc.retry.delay.Reset()

	p := c.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = cap(tasks) / 2

	// 循环从BackendConn的input这个chan取redis请求(客户端发来的请求)
	// 这里bc.input中的request是session中readerLoop写入的
	for r := range bc.input {
		if r.IsReadOnly() && r.IsBroken() {
			bc.setResponse(r, nil, ErrRequestIsBroken)
			continue
		}

		if err := p.EncodeMultiBulk(r.Multi); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		// EncodeMultiBulk直接将请求发送给codis-server
		// 此处flush将请求刷到后端redis的socket中, 但write并不阻塞等待后端redis的回复
		if err := p.Flush(len(bc.input) == 0); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		} else {
			// 所有请求写入tasks这个chan(这里写入的request还是session最初的那个request)
			// 上面EncodeMultiBulk将请求发送到了redis, 但是没有直接同步的拿取回包
			// 此处已经发送命令的r写入tasks, 此时write线程会从range阻塞中恢复, 但有可能继续阻塞在decode(socket read)
			// 继续等待后端redis的回复触发socket可读
			tasks <- r
		}
	}
	return nil
}

type sharedBackendConn struct {
	addr string
	host []byte
	port []byte

	// 所属的池
	owner *sharedBackendConnPool
	// [database][parallel] 以database为维度创建连接
	// parallel表示连接的并行数, 每个库上的链接数
	conns [][]*BackendConn

	// if parallel=1 use this  是conns这个二维切片每一列的第一个
	single []*BackendConn

	//当前sharedBackendConn的引用计数，非正数的时候表明关闭, 每多一个引用就加一
	refcnt int
}

func newSharedBackendConn(addr string, pool *sharedBackendConnPool) *sharedBackendConn {
	// 拆分ip port
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.ErrorErrorf(err, "split host-port failed, address = %s", addr)
	}
	s := &sharedBackendConn{
		addr: addr,
		host: []byte(host), port: []byte(port),
	}
	//确认新建的sharedBackendConn所属于的pool
	s.owner = pool
	//len和cap都默认为16的二维切片
	s.conns = make([][]*BackendConn, pool.config.BackendNumberDatabases)
	//range用一个参数遍历二维切片，datebase是0到15
	for database := range s.conns {
		//len和cap都默认为1的一维切片
		parallel := make([]*BackendConn, pool.parallel)
		//只有parallel[0]
		for i := range parallel {
			parallel[i] = NewBackendConn(addr, database, pool.config)
		}
		s.conns[database] = parallel
	}
	if pool.parallel == 1 {
		s.single = make([]*BackendConn, len(s.conns))
		for database := range s.conns {
			s.single[database] = s.conns[database][0]
		}
	}
	//新建之后，这个SharedBackendConn的引用次数就置为1
	s.refcnt = 1
	return s
}

func (s *sharedBackendConn) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

func (s *sharedBackendConn) Release() {
	if s == nil {
		return
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	} else {
		s.refcnt--
	}
	if s.refcnt != 0 {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.Close()
		}
	}
	delete(s.owner.pool, s.addr)
}

func (s *sharedBackendConn) Retain() *sharedBackendConn {
	if s == nil {
		return nil
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed")
	} else {
		s.refcnt++
	}
	return s
}

func (s *sharedBackendConn) KeepAlive() {
	if s == nil {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.KeepAlive()
		}
	}
}

func (s *sharedBackendConn) BackendConn(database int32, seed uint, must bool) *BackendConn {
	if s == nil {
		return nil
	}

	// 单数据库
	if s.single != nil {
		bc := s.single[database]
		if must || bc.IsConnected() {
			return bc
		}
		return nil
	}

	// 多数据库
	var parallel = s.conns[database]

	var i = seed
	for range parallel {
		i = (i + 1) % uint(len(parallel))
		if bc := parallel[i]; bc.IsConnected() {
			return bc
		}
	}
	if !must {
		return nil
	}
	return parallel[0]
}

type sharedBackendConnPool struct {
	// 从启动配置文件参数封装的config
	config   *Config
	parallel int   //并行个数? 默认为1

	// 这里可以做快慢分离
	//quick int

	// pool[addr] 与后端redis的链接
	// backent是以slot为单位分配的, 如果两个slot落在同一redis实例, 则公共一个backend
	pool map[string]*sharedBackendConn
}

func newSharedBackendConnPool(config *Config, parallel int) *sharedBackendConnPool {
	p := &sharedBackendConnPool{
		config: config, parallel: math2.MaxInt(1, parallel),
	}
	p.pool = make(map[string]*sharedBackendConn)
	return p
}

func (p *sharedBackendConnPool) KeepAlive() {
	for _, bc := range p.pool {
		bc.KeepAlive()
	}
}

func (p *sharedBackendConnPool) Get(addr string) *sharedBackendConn {
	return p.pool[addr]
}

// 根据后端的addr取对应的sharedBackendConn
func (p *sharedBackendConnPool) Retain(addr string) *sharedBackendConn {
	// 首先从pool中直接拉取, 取到的话引用计数+1
	if bc := p.pool[addr]; bc != nil {
		// 引用计数加一
		return bc.Retain()
	} else {
		// 取不到就新建, 然后让方入pool里面
		bc = newSharedBackendConn(addr, p)
		p.pool[addr] = bc
		return bc
	}
}
