// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"time"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/proxy"
	"github.com/CodisLabs/codis/pkg/utils/log"
	"github.com/CodisLabs/codis/pkg/utils/redis"
	"github.com/CodisLabs/codis/pkg/utils/rpc"
	"github.com/CodisLabs/codis/pkg/utils/sync2"
)

type RedisStats struct {
	//储存了集群中Redis服务器的各种信息和统计数值，详见redis的info命令
	Stats map[string]string `json:"stats,omitempty"`
	Error *rpc.RemoteError  `json:"error,omitempty"`

	Sentinel map[string]*redis.SentinelGroup `json:"sentinel,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newRedisStats(addr string, timeout time.Duration, do func(addr string) (*RedisStats, error)) *RedisStats {
	var ch = make(chan struct{})
	stats := &RedisStats{}

	go func() {
		defer close(ch)
		p, err := do(addr)
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats, stats.Sentinel = p.Stats, p.Sentinel
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &RedisStats{Timeout: true}
	}
}

func (s *Topom) RefreshRedisStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//从Topom.cache缓存中读出slots，group，proxy，sentinel等信息封装在context struct中
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	goStats := func(addr string, do func(addr string) (*RedisStats, error)) {
		fut.Add()
		go func() {
			stats := s.newRedisStats(addr, timeout, do)
			stats.UnixTime = time.Now().Unix()
			//vmap中添加键为addr，值为RedisStats的map
			fut.Done(addr, stats)
		}()
	}

	//遍历ctx中的group，再遍历每个group中的Server。如果对group和Server结构不清楚的，可以看看/pkg/models/group.go文件
	//每个Group除了id，还有一个属性就是GroupServer。每个GroupServer有自己的地址、数据中心、action等等
	for _, g := range ctx.group {
		for _, x := range g.Servers {
			goStats(x.Addr, func(addr string) (*RedisStats, error) {
				//前面我们已经说过，Topom中有三个redis pool，分别是action,stats,ha。pool本质上就是map[String]*list.List。
				//这个是从stats的pool中根据Server的地址从pool中取redis client，如果没有client，就创建
				//然后加入到pool里面，并通过Info命令获取详细信息。整个流程和下面的sentinel类似，这里就不放具体的方法实现了
				m, err := s.stats.redisp.InfoFull(addr)
				if err != nil {
					return nil, err
				}
				return &RedisStats{Stats: m}, nil
			})
		}
	}

	//通过sentinel维护codis集群中每一组的主备关系
	for _, server := range ctx.sentinel.Servers {
		goStats(server, func(addr string) (*RedisStats, error) {
			c, err := s.ha.redisp.GetClient(addr)
			if err != nil {
				return nil, err
			}
			//实际上就是将client加入到Pool的pool属性里面去，pool本质上就是map[String]*list.List
			//键是client的addr,值是client本身
			//如果client不存在，就新建一个空的list
			defer s.ha.redisp.PutClient(c)
			m, err := c.Info()
			if err != nil {
				return nil, err
			}
			//sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
			//获得map[string]*SentinelGroup，键是每一组的master的名字，SentinelGroup则是主从对
			sentinel := redis.NewSentinel(s.config.ProductName, s.config.ProductAuth)
			p, err := sentinel.MastersAndSlavesClient(c)
			if err != nil {
				return nil, err
			}
			return &RedisStats{Stats: m, Sentinel: p}, nil
		})
	}

	//前面的所有gostats执行完之后，遍历Future的vmap，将值赋给Topom.stats.servers
	go func() {
		stats := make(map[string]*RedisStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*RedisStats)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		s.stats.servers = stats
	}()
	return &fut, nil
}

type ProxyStats struct {
	Stats *proxy.Stats     `json:"stats,omitempty"`
	Error *rpc.RemoteError `json:"error,omitempty"`

	UnixTime int64 `json:"unixtime"`
	Timeout  bool  `json:"timeout,omitempty"`
}

func (s *Topom) newProxyStats(p *models.Proxy, timeout time.Duration) *ProxyStats {
	var ch = make(chan struct{})
	stats := &ProxyStats{}

	go func() {
		defer close(ch)
		x, err := s.newProxyClient(p).StatsSimple()
		if err != nil {
			stats.Error = rpc.NewRemoteError(err)
		} else {
			stats.Stats = x
		}
	}()

	select {
	case <-ch:
		return stats
	case <-time.After(timeout):
		return &ProxyStats{Timeout: true}
	}
}

//刷新proxy状态，将每一个proxy.Token和ProxyStats的关系map对应起来，存入Topom.stats.proxies中
func (s *Topom) RefreshProxyStats(timeout time.Duration) (*sync2.Future, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	//在这个构造函数中将判断cache中的数据是否为空，为空的话就通过store从zk中取出填进cache
	ctx, err := s.newContext()
	if err != nil {
		return nil, err
	}
	var fut sync2.Future
	//如果手动添加的proxy，这里ctx.proxy就不为空了
	for _, p := range ctx.proxy {
		//fut中的waitsGroup加1
		fut.Add()
		go func(p *models.Proxy) {
			stats := s.newProxyStats(p, timeout)
			stats.UnixTime = time.Now().Unix()
			//在fut的vmap属性中，添加以proxy.Token为键，ProxyStats为值的map，并将waitsGroup减1
			fut.Done(p.Token, stats)

			switch x := stats.Stats; {
			case x == nil:
			case x.Closed || x.Online:
				//如果一个proxy因为某种情况出现error，被运维重启之后，处于waiting状态，会调用OnlineProxy方法将proxy重新添加到集群中
			default:
				if err := s.OnlineProxy(p.AdminAddr); err != nil {
					log.WarnErrorf(err, "auto online proxy-[%s] failed", p.Token)
				}
			}
		}(p)
	}
	// 当所有proxy.Token和ProxyStats的关系map建立好之后，存到Topom.stats.proxies中
	// 下面的线程会阻塞在fut.Wait(), 当上面的循环将每个stats都处理好后, 会执行下面的go
	go func() {
		stats := make(map[string]*ProxyStats)
		for k, v := range fut.Wait() {
			stats[k] = v.(*ProxyStats)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		//Topom的stats结构中的proxies属性，存储了完整的stats信息
		s.stats.proxies = stats
	}()
	return &fut, nil
}
