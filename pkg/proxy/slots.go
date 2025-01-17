// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proxy

import (
	"sync"

	"github.com/CodisLabs/codis/pkg/models"
)

type Slot struct {
	// slot id about hash
	id   int
	lock struct {
		hold bool
		sync.RWMutex
	}

	// 同步变量
	// 设置fillslots的时候会wait
	refs sync.WaitGroup

	// 主从切换?
	switched bool

	//backend 表示当前到后端redis(普通命令到后端连接)
	//migrate 表示从何处迁移(数据迁移后端连接)
	backend, migrate struct {
		id int
		bc *sharedBackendConn
	}
	//从库连接池
	replicaGroups [][]*sharedBackendConn

	// 请求路由方法
	method forwardMethod
}

func (s *Slot) snapshot() *models.Slot {
	var m = &models.Slot{
		Id:     s.id,
		Locked: s.lock.hold,

		BackendAddr:        s.backend.bc.Addr(),
		BackendAddrGroupId: s.backend.id,
		MigrateFrom:        s.migrate.bc.Addr(),
		MigrateFromGroupId: s.migrate.id,
		ForwardMethod:      s.method.GetId(),
	}
	for i := range s.replicaGroups {
		var group []string
		for _, bc := range s.replicaGroups[i] {
			group = append(group, bc.Addr())
		}
		m.ReplicaGroups = append(m.ReplicaGroups, group)
	}
	return m
}

func (s *Slot) blockAndWait() {
	if !s.lock.hold {
		s.lock.hold = true
		s.lock.Lock()
	}
	s.refs.Wait()
}

func (s *Slot) unblock() {
	if !s.lock.hold {
		return
	}
	s.lock.hold = false
	s.lock.Unlock()
}

func (s *Slot) forward(r *Request, hkey []byte) error {
	return s.method.Forward(s, r, hkey)
}
