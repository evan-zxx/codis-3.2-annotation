// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
)

const MaxSlotNum = models.MaxSlotNum

//存储了集群中所有的sharedBackendConnPool和slot
//用于将redis请求转发给相应的slot进行处理
type Router struct {
	mu sync.RWMutex

	pool struct {
		// 后端与redis通信的链接池 主从
		primary *sharedBackendConnPool
		replica *sharedBackendConnPool
	}
	// key对应的槽
	slots [MaxSlotNum]Slot

	config *Config
	online bool
	closed bool
}

func NewRouter(config *Config) *Router {
	s := &Router{config: config}
	s.pool.primary = newSharedBackendConnPool(config, config.BackendPrimaryParallel)
	s.pool.replica = newSharedBackendConnPool(config, config.BackendReplicaParallel)
	for i := range s.slots {
		s.slots[i].id = i
		s.slots[i].method = &forwardSync{}
	}
	return s
}

func (s *Router) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.online = true
}

func (s *Router) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true

	for i := range s.slots {
		s.fillSlot(&models.Slot{Id: i}, false, nil)
	}
}

func (s *Router) GetSlots() []*models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	slots := make([]*models.Slot, MaxSlotNum)
	for i := range s.slots {
		slots[i] = s.slots[i].snapshot()
	}
	return slots
}

func (s *Router) GetSlot(id int) *models.Slot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if id < 0 || id >= MaxSlotNum {
		return nil
	}
	slot := &s.slots[id]
	return slot.snapshot()
}

func (s *Router) HasSwitched() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.slots {
		if s.slots[i].switched {
			return true
		}
	}
	return false
}

var (
	ErrClosedRouter  = errors.New("use of closed router")
	ErrInvalidSlotId = errors.New("use of invalid slot id")
	ErrInvalidMethod = errors.New("use of invalid forwarder method")
)

func (s *Router) FillSlot(m *models.Slot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 参数校验
	// 准备根据之前的1024个models.Slot，创建1024个/pkg/proxy/slots.go中的Slot
	if s.closed {
		return ErrClosedRouter
	}
	if m.Id < 0 || m.Id >= MaxSlotNum {
		return ErrInvalidSlotId
	}
	var method forwardMethod
	switch m.ForwardMethod {
	default:
		return ErrInvalidMethod
	case models.ForwardSync:
		method = &forwardSync{}
	case models.ForwardSemiAsync:
		method = &forwardSemiAsync{}
	}
	s.fillSlot(m, false, method)
	return nil
}

func (s *Router) KeepAlive() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed {
		return ErrClosedRouter
	}
	s.pool.primary.KeepAlive()
	s.pool.replica.KeepAlive()
	return nil
}

func (s *Router) isOnline() bool {
	return s.online && !s.closed
}

//将某个request分发给各个具体的slot进行处理的方法主要有三个，都是由Router来完成：
//根据key进行转发
func (s *Router) dispatch(r *Request) error {
	hkey := getHashKey(r.Multi, r.OpStr)
	var id = Hash(hkey) % MaxSlotNum
	slot := &s.slots[id]
	//将请求分发到相应的slot
	return slot.forward(r, hkey)
}

//将request发到指定slot
func (s *Router) dispatchSlot(r *Request, id int) error {
	if id < 0 || id >= MaxSlotNum {
		return ErrInvalidSlotId
	}
	slot := &s.slots[id]
	return slot.forward(r, nil)
}

//这个直接指明了redis服务器地址的请求，如果找不到就返回false
func (s *Router) dispatchAddr(r *Request, addr string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if bc := s.pool.primary.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	if bc := s.pool.replica.Get(addr).BackendConn(r.Database, r.Seed16(), false); bc != nil {
		bc.PushBack(r)
		return true
	}
	return false
}


//这个阶段主要是初始化槽/ 或者dashboard重新迁移了槽 刷新映射关系
//初始化之后 每个slot都被分配了相应的backendConn，只不过此时每个backendConn都为空
func (s *Router) fillSlot(m *models.Slot, switched bool, method forwardMethod) {
	slot := &s.slots[m.Id]
	// 将slot的lock.hold属性设为true，并加锁
	// 等后端命令执行完之后这里的线程会被唤醒 继续进行更新映射关系
	slot.blockAndWait()

	// reset
	// 清空models.Slot里面的backendConn
	// 这里的bc就是我们之前提到过的处理redis请求的sharedBackendConn 第一次进来的时候，backend为空，这里直接返回
	slot.backend.bc.Release()
	slot.backend.bc = nil
	slot.backend.id = 0
	slot.migrate.bc.Release()
	slot.migrate.bc = nil
	slot.migrate.id = 0
	for i := range slot.replicaGroups {
		for _, bc := range slot.replicaGroups[i] {
			bc.Release()
		}
	}
	slot.replicaGroups = nil

	//false
	slot.switched = switched

	//初始阶段addr和from都是空字符串
	//这里的addr就是迁移出的后端redis地址
	if addr := m.BackendAddr; len(addr) != 0 {
		// 创建后端的链接 通过codis-server地址获取SharedBackendConn
		// 从Router的primary sharedBackendConnPool中取出addr对应的sharedBackendConn，如果没有就新建并放入，也相当于初始化了
		slot.backend.bc = s.pool.primary.Retain(addr)
		slot.backend.id = m.BackendAddrGroupId
	}
	if from := m.MigrateFrom; len(from) != 0 {
		slot.migrate.bc = s.pool.primary.Retain(from)
		slot.migrate.id = m.MigrateFromGroupId
	}

	if !s.config.BackendPrimaryOnly {
		for i := range m.ReplicaGroups {
			var group []*sharedBackendConn
			for _, addr := range m.ReplicaGroups[i] {
				group = append(group, s.pool.replica.Retain(addr))
			}
			if len(group) == 0 {
				continue
			}
			slot.replicaGroups = append(slot.replicaGroups, group)
		}
	}
	if method != nil {
		slot.method = method
	}

	if !m.Locked {
		slot.unblock()
	}
	if !s.closed {
		if slot.migrate.bc != nil {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, backend.addr = %s, migrate.from = %s, locked = %t",
					slot.id, slot.backend.bc.Addr(), slot.migrate.bc.Addr(), slot.lock.hold)
			}
		} else {
			if switched {
				log.Warnf("fill slot %04d, backend.addr = %s, locked = %t, +switched",
					slot.id, slot.backend.bc.Addr(), slot.lock.hold)
			} else {
				log.Warnf("fill slot %04d, backend.addr = %s, locked = %t",
					slot.id, slot.backend.bc.Addr(), slot.lock.hold)
			}
		}
	}
}

func (s *Router) SwitchMasters(masters map[int]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosedRouter
	}
	cache := &redis.InfoCache{
		Auth: s.config.ProductAuth, Timeout: time.Millisecond * 100,
	}
	for i := range s.slots {
		s.trySwitchMaster(i, masters, cache)
	}
	return nil
}

func (s *Router) trySwitchMaster(id int, masters map[int]string, cache *redis.InfoCache) {
	var switched bool
	var m = s.slots[id].snapshot()

	hasSameRunId := func(addr1, addr2 string) bool {
		if addr1 != addr2 {
			rid1 := cache.GetRunId(addr1)
			rid2 := cache.GetRunId(addr2)
			return rid1 != "" && rid1 == rid2
		}
		return true
	}

	if addr := masters[m.BackendAddrGroupId]; addr != "" {
		if !hasSameRunId(addr, m.BackendAddr) {
			m.BackendAddr, switched = addr, true
		}
	}
	if addr := masters[m.MigrateFromGroupId]; addr != "" {
		if !hasSameRunId(addr, m.MigrateFrom) {
			m.MigrateFrom, switched = addr, true
		}
	}
	if switched {
		s.fillSlot(m, true, nil)
	}
}
