package client

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

const ServerIP = "127.0.0.1"

var once sync.Once

type Client struct {
	wg              sync.WaitGroup
	peerIsMap       []string
	chOperationConn chan *rpc.Client
	chOpt           chan interface{} //操作channel
	init            bool
}
type Clienter interface {
	Start()
	ShutDown()
}

func NewClient() *Client {
	return &Client{
		wg: sync.WaitGroup{},
		peerIsMap: []string{
			":2333",
			":2334",
			":2335",
			":2336",
			":2337",
		},
		chOperationConn: make(chan *rpc.Client, 1),
		chOpt:           make(chan interface{}, 1),
	}
}

//因为是刚开始。所以需要统一发送消息让各结点准备选举
func (c *Client) Start() {
	for _, addr := range c.peerIsMap {
		c.wg.Add(1)
		go func(addr string) {
			defer c.wg.Done()
			if _, err := net.Dial("tcp", ServerIP+addr); err != nil {
				fmt.Println(err)
				return
			}
		}(addr)
	}
	c.wg.Wait()
	fmt.Println("所有结点通知启动完毕")
	c.waitStart("请等待raft集群启动倒计时")
	c.linkCluster()
}
func (c *Client) waitStart(s string) {
	ticker := time.NewTicker(time.Second * 1)
	var count = 3
	for {
		if count == -1 {
			return
		}
		<-ticker.C
		fmt.Printf("\r%d秒后raft集群启动完毕。", count)
		count -= 1
	}
}

//client 会直接跟server的cluster的leader链接在一起，
//1. 先随机链接一个节点， 判断当前节点状态是不是主节点 如果不是就由当前节点告知主节点编号给客户端
//2. 客户端得到节点返回的编号，对其链接
//3. 当换主的时候，应该主动告知客户端切换，还是应该等待客户端自己察觉呢？
type LinkClientArgs struct {
	Command interface{}
}
type LinkClientReply struct {
	IsLeader bool
	LeaderId int
}

var wg sync.WaitGroup

func (c *Client) linkCluster() {
	fmt.Println("开始寻找主节点")
loop:
	//for {
	var isOk = 0
	for _, addr := range c.peerIsMap {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if conn, err := rpc.Dial("tcp", ServerIP+addr); err != nil {
				//fmt.Printf("[%s] client.linkCluster.rpc is failed:%s \n", addr, err)
				return
			} else {
				var args LinkClientArgs
				var reply LinkClientReply
				err := conn.Call("ConsensusModule.LinkServer", args, &reply)
				if err != nil {
					//fmt.Printf("[%s] clinet.linkCluster.rpc.call is failed:%s \n", addr, err)
					return
				}
				if reply.IsLeader {
					fmt.Printf("当前节点[%d]为主节点\n", reply.LeaderId)
					isOk++
					c.chOperationConn <- conn
					return
				}
			}
		}(addr)
	}
	//}
	wg.Wait()
	if isOk == 0 {
		goto loop
	}
	if !c.init {
		go c.operation()
		c.init = true
	}
	//c.operation() //第一次联系到集群的主server之后，就开启一个协程 一直等待收链接做操作
	//fmt.Println("初始化完毕")
}

//是一个协程， 主要就是监听操作，
//拿着操作向下执行，如果调用成功就继续执行
//如果调用失败 就调用链接集群的操作然后拿到新的conn 重新执行，
//直到这个操作消费成功
func (c *Client) operation() {
	var conn *rpc.Client
	for {
		select {
		case opt := <-c.chOpt:
			arg := LinkClientArgs{Command: opt}
			var reply LinkClientReply
		getConn:
			if conn == nil {
				conn = <-c.chOperationConn
			}
			err := conn.Call("ConsensusModule.Set", arg, &reply)
			if err != nil || !reply.IsLeader {
				fmt.Println("进入到错误分支： ", err, reply.IsLeader)
				c.linkCluster() //重新链接集群中的主结点
				conn = <-c.chOperationConn
				goto getConn
			} else {
				fmt.Println(reply.LeaderId, "结点是leader")
			}

		}
	}
}

//暴露给客户端的操作。
//设计思路 set传入一个opt 然后丢给operation
//operation一直监听着管道的消息 如果换了主
func (c *Client) Set(opt interface{}) {
	//fmt.Println("sss")
	if opt == nil {
		return
	} else {
		c.chOpt <- opt
	}
}
func (c *Client) ShutDown() {

}

//loop:
//conn := <-c.chOperationConn
//fmt.Println("根据当前的conn做操作")
////拿着这个connect做操作，如果操作失败，就再次寻找leader
//ticker := time.NewTicker(time.Second * 2)
//for {
//<-ticker.C
//var args LinkClientArgs
//var reply LinkClientReply
//err := conn.Call("ConsensusModule.Get", args, &reply)
//if err != nil || !reply.IsLeader {
//fmt.Println("链接失败准备选主", err)
//c.linkCluster() //选主
//goto loop
//} else {
//fmt.Println("正常操作") //这里就链接正常的操作
//}
//}
