### 目录

```
.
├── Godeps               #工具管理包
├── admin                #后台服务启动脚本
├── ansible              #ansible自动化运维工具，可以实现快速部署
├── bin                  #执行文件目录
│   └── assets           #codis-fe前端文件
├── cmd                  #脚本源码入口文件目录
│   ├── admin            #codis-admin入口文件目录
│   ├── dashboard        #codis-dashboard入口文件目录
│   ├── fe              #codis-fe入口文件目录
│   ├── ha              #codis-ha入口文件目录
│   └── proxy           #codis-proxy入口文件目录
├── config              #配置文件目录
├── deploy              #部署配置目录，
├── doc                 #文档
├── example             #示例目录，一个小例子
├── extern              #修改版本的redis源代码
├── kubernetes          #k8s管理目录
│   └── zookeeper       #zk配置目录
├── log                 #日志文件
├── pkg                 #核心代码部门
├── scripts             #运行脚本
└── vendor              #管理工具外部支持包
```