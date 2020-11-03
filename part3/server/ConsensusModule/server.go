package ConsensusModule

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var (
	//结点id
	peerIds = []int{1, 2, 3, 4, 5}
	//结点id对应的ip地址
	peerIsMap = map[int]string{
		1: ":2333",
		2: ":2334",
		3: ":2335",
		4: ":2336",
		5: ":2337",
	}
	//通知服务的管道
	ready = make(chan struct{})
)

type RPCProxy struct {
	cm *ConsensusModule
}
type server struct {
	wg        sync.WaitGroup
	listener  net.Listener
	once      sync.Once
	mut       sync.Mutex
	cm        *ConsensusModule
	rpcProxy  *RPCProxy
	rpcServer *rpc.Server
}

func NewServer() *server {
	return &server{}
}
func (s *server) sayHello(addr string, conn net.Conn) {
	if _, err2 := conn.Write([]byte(addr + "is ok")); err2 != nil {
		fmt.Println("conn write is failed ", err2)
	}
}

func (s *server) Serve(serverId int) {
	peerAddr, hit := peerIsMap[serverId]
	if !hit {
		fmt.Println("peerIsMap haven't this peer Id ", serverId)
		return
	}
	s.mut.Lock()
	//关于一致性模块的创建和rpc的注册
	s.cm = NewConsensusModule(serverId, peerIds, ready, s)
	s.rpcProxy = &RPCProxy{cm: s.cm}
	s.rpcServer = rpc.NewServer()
	if e := s.rpcServer.RegisterName("ConsensusModule", s.rpcProxy); e != nil {
		fmt.Println("registerName is failed ", e)
	}
	//fmt.Println("zc 成功")
	//cm模块创建 rpc注册结束。
	var err error
	if s.listener, err = net.Listen("tcp", peerAddr); err != nil {
		fmt.Println("listen is failed ...", err)
		return
	}
	fmt.Printf("peer[%d] = %s is ready\n", serverId, peerAddr)
	s.mut.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err2 := s.listener.Accept()
			if err2 != nil {
				fmt.Println("listener accept is failed ..", err2)
				continue
			}
			//fmt.Println("当前接收到的是来自：", conn.RemoteAddr().String())
			//read只能执行一次，也就是第一次执行
			s.once.Do(s.readyAllPeer)
			s.wg.Add(1)
			////将
			go func() {
				defer s.wg.Done()
				s.rpcServer.ServeConn(conn)
				//conn.Write([]byte("注册成功
			}()
		}
	}()
	s.wg.Wait()
}

func (s *server) readyAllPeer() {
	fmt.Println("信号量发出，在初始状态下所有结点可以开始选举了")
	ready <- struct{}{}
}

func (s *server) call(peerId int, rpcFuncName string, args, reply interface{}) error {
	s.mut.Lock()
	peerAddress, hit := peerIsMap[peerId]
	s.mut.Unlock()
	if !hit {
		return fmt.Errorf("no peerId[%d]", peerId)
	}
	//fmt.Println("准备拨号->", "127.0.0.1"+peerAddress)
	if client, err := rpc.Dial("tcp", "127.0.0.1"+peerAddress); err != nil { //因该就是卡在这里了
		return fmt.Errorf("rpc.dial is failed :%s", err)
	} else {
		return client.Call(rpcFuncName, args, reply)
	}
}

func (r *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	//fmt.Println("ConsensusModule.RequestVote")
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			fmt.Println("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			fmt.Println("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return r.cm.RequestVote(args, reply)
}
func (r *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	//fmt.Println("接收到心跳")
	if len(os.Getenv("RAFT_UNRELIABLE_RPC")) > 0 {
		dice := rand.Intn(10)
		if dice == 9 {
			fmt.Println("drop RequestVote")
			return fmt.Errorf("RPC failed")
		} else if dice == 8 {
			fmt.Println("delay RequestVote")
			time.Sleep(75 * time.Millisecond)
		}
	} else {
		time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	}
	return r.cm.AppendEntries(args, reply)
}

//LinkClient用来给客户端链接，客户端通过这个方法得到当前节点是不是主节点，
//如果是就返回success
//不是的话。就返回failed
type LinkClientArgs struct {
	Command interface{}
}
type LinkClientReply struct {
	IsLeader bool
	LeaderId int
}

func (r *RPCProxy) LinkServer(args LinkClientArgs, reply *LinkClientReply) error {
	//r.cm.consoleLog("客户端链接成功，当前节点为[%d],状态为[%d]", r.cm.Id, r.cm.state)
	if r.cm.state == Leader {
		reply.IsLeader = true
		reply.LeaderId = r.cm.Id
	}
	return nil
}

func (r *RPCProxy) Get(args LinkClientArgs, reply *LinkClientReply) error {
	if r.cm.state == Leader {
		reply.IsLeader = true
		reply.LeaderId = r.cm.Id
	} else {
		reply.IsLeader = false
		return nil
	}
	return nil
}

func (r *RPCProxy) Set(arg LinkClientArgs, reply *LinkClientReply) error {
	reply.IsLeader, reply.LeaderId = r.cm.Submit(arg.Command)
	return nil
}
