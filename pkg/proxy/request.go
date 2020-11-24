// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"unsafe"

	"github.com/CodisLabs/codis/pkg/proxy/redis"
	"github.com/CodisLabs/codis/pkg/utils/sync2/atomic2"
)


// 请求处理的内部实体类
type Request struct {
	// 最开始客户端的请求(redis命令数组)
	Multi []*redis.Resp

	// 批量子请求控制器
	// 控制前端请求处理进度 前端传给后端请求时+1 后端处理完之后-1
	Batch *sync.WaitGroup

	// TODO: 这里有误:当需要进行命令拆分, 例如mget, 会将不同的命令路由到不同的后端server/slots
	// 其实是保证group slots映射关系. 每次在fillSlots之前会wait()住
	Group *sync.WaitGroup

	Broken *atomic2.Bool

	// redis请求的具体命令
	OpStr string
	// 命令对应标志 快慢/读写
	OpFlag

	// 当前哪个数据库 select相关
	Database int32
	UnixNano int64

	// 最终恢复给前端的rsp
	*redis.Resp
	Err error

	Coalesce func() error
}

func (r *Request) IsBroken() bool {
	return r.Broken != nil && r.Broken.IsTrue()
}

func (r *Request) MakeSubRequest(n int) []Request {
	var sub = make([]Request, n)
	for i := range sub {
		x := &sub[i]
		x.Batch = r.Batch
		x.OpStr = r.OpStr
		x.OpFlag = r.OpFlag
		x.Broken = r.Broken
		x.Database = r.Database
		x.UnixNano = r.UnixNano
	}
	return sub
}

const GOLDEN_RATIO_PRIME_32 = 0x9e370001

func (r *Request) Seed16() uint {
	h32 := uint32(r.UnixNano) + uint32(uintptr(unsafe.Pointer(r)))
	h32 *= GOLDEN_RATIO_PRIME_32
	return uint(h32 >> 16)
}

// 每一个session都会有一个RequestChan
type RequestChan struct {
	lock sync.Mutex

	//sync.NewCond(&RequestChan.lock)
	//如果RequestChan为空，就让goroutine wait；如果向RequestChan放入了一个请求，并且有goroutine在等待，就唤醒一个
	cond *sync.Cond

	// 记录已经处理过的Request 向客户端返回最终结果时, 每次取c.data[0]发送给前端
	data []*Request
	// 存储客户端发送来的请求
	buff []*Request

	waits  int
	closed bool
}

const DefaultRequestChanBuffer = 128

func NewRequestChan() *RequestChan {
	return NewRequestChanBuffer(0)
}

func NewRequestChanBuffer(n int) *RequestChan {
	if n <= 0 {
		n = DefaultRequestChanBuffer
	}
	var ch = &RequestChan{
		buff: make([]*Request, n),
	}
	ch.cond = sync.NewCond(&ch.lock)
	return ch
}

func (c *RequestChan) Close() {
	c.lock.Lock()
	if !c.closed {
		c.closed = true
		c.cond.Broadcast()
	}
	c.lock.Unlock()
}

func (c *RequestChan) Buffered() int {
	c.lock.Lock()
	n := len(c.data)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PushBack(r *Request) int {
	c.lock.Lock()
	n := c.lockedPushBack(r)
	c.lock.Unlock()
	return n
}

func (c *RequestChan) PopFront() (*Request, bool) {
	c.lock.Lock()
	r, ok := c.lockedPopFront()
	c.lock.Unlock()
	return r, ok
}

//把request（此时已经处理完毕，将resp设置为request的一个参数）添加到当前Session中之前创建的RequestChan中
// loopWriter后面再遍历RequestChan取出所有请求及结果
func (c *RequestChan) lockedPushBack(r *Request) int {
	if c.closed {
		panic("send on closed chan")
	}

	// RequestChan的waits不为0的时候（也就是在RequestChan上等待的request数量不为0时) 唤醒一个在cond上等待的goroutine
	// 这里的意思是，如果向requestChan中放入了请求，就将一个在cond上等待取出的goroutine唤醒

	if c.waits != 0 {
		c.cond.Signal()
	}
	//将request添加到RequestChan的data []*Request切片中，用于记录处理过的请求
	c.data = append(c.data, r)
	return len(c.data)
}

func (c *RequestChan) lockedPopFront() (*Request, bool) {
	for len(c.data) == 0 {
		if c.closed {
			return nil, false
		}
		// 取到c.buff的第一个元素
		c.data = c.buff[:0]
		c.waits++
		c.cond.Wait()
		// 由loopReader.lockedPushBack中唤醒到这里
		c.waits--
	}
	var r = c.data[0]
	// 获取c.data[1...len]所有的元素赋值到c.data(也就是去掉第一个元素)
	c.data, c.data[0] = c.data[1:], nil
	return r, true
}

func (c *RequestChan) IsEmpty() bool {
	return c.Buffered() == 0
}

func (c *RequestChan) PopFrontAll(onRequest func(r *Request) error) error {
	for {
		// 阻塞到loopReader.push
		r, ok := c.PopFront()
		if ok {
			// 这里执行了传进来的onRequest
			if err := onRequest(r); err != nil {
				return err
			}
		} else {
			return nil
		}
	}
}

func (c *RequestChan) PopFrontAllVoid(onRequest func(r *Request)) {
	c.PopFrontAll(func(r *Request) error {
		onRequest(r)
		return nil
	})
}
