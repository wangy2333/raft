package client

import (
	"fmt"
	"net"
	"sync"
)

var peerIsMap = []string{}

type Client struct {
	wg        sync.WaitGroup
	peerIsMap []string
}
type Clienter interface {
	Start()
	ShutDown()
}

func NewClient() *Client {
	return &Client{
		wg: sync.WaitGroup{},
		peerIsMap: []string{
			"127.0.0.1:2333",
			"127.0.0.1:2334",
			"127.0.0.1:2335",
			"127.0.0.1:2336",
			"127.0.0.1:2337",
		},
	}
}

//因为是刚开始。所以需要统一发送消息让各结点准备选举
func (c *Client) Start() {
	for _, addr := range c.peerIsMap {
		c.wg.Add(1)
		go func(addr string) {
			conn, err := net.Dial("tcp", addr)
			defer conn.Close()
			defer c.wg.Done()
			if err != nil {
				fmt.Println(err)
				return
			}
			if _, err := conn.Write([]byte("hello")); err != nil {
				fmt.Println(err)
				return
			}
			buf := [512]byte{}
			n, err3 := conn.Read(buf[:])
			if err3 != nil {
				fmt.Println("recv failed, err:", err)
				return
			}
			fmt.Println(string(buf[:n]))
		}(addr)
	}
	fmt.Println("所有结点通知启动完毕")
	c.wg.Wait()
}
func (c *Client) ShutDown() {

}
