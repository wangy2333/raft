package ConsensusModule

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Follower = iota
	Candidate
	Leader
	Dead
)

type CMState int

func (cms CMState) string() string {
	switch cms {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "bad state "
	}
}

type ConsensusModule struct {
	//cm模块id
	Id int
	//当前给哪个结点投票投票
	voteFor int
	//结点名单
	peerIds []int
	//任期
	currentTerm int
	//锁
	mut sync.Mutex
	//等待组
	wg sync.WaitGroup
	//当前的状态
	state CMState
	//网络服务
	server *server
	//选举重置时间
	electionResetEvent time.Time
}

func NewConsensusModule(serverId int, peerIds []int, ready <-chan struct{}, srv *server) *ConsensusModule {
	cm := &ConsensusModule{}
	cm.Id = serverId
	cm.voteFor = -1 //初始-1代表没有投票过
	cm.currentTerm = 0
	cm.state = Follower
	cm.peerIds = peerIds
	cm.server = srv
	go func() {
		//fmt.Println("等待信号")
		<-ready
		cm.mut.Lock()
		cm.electionResetEvent = time.Now()
		cm.mut.Unlock()
		cm.runElectionTimer()
	}()
	return cm
}
func (cm *ConsensusModule) runElectionTimer() {
	fmt.Printf("[%d]号节点 开始选举\n", cm.Id)
	//计算一个随即超时时间
	timeoutDuration := cm.electionTimeout()
	//fmt.Println("周期：", timeoutDuration)
	cm.mut.Lock()
	saveTerm := cm.currentTerm
	cm.mut.Unlock()
	ticker := time.NewTicker(10 * time.Millisecond * 10)
	defer ticker.Stop()
	//选举停止条件：
	// - 如果当前的状态是Leader 就直接返回
	// - 发现比自己任期大的节点存在
	// - 当前一个周期之后身份还没有发生变化，任期也相同，就申请选举
	for {
		<-ticker.C //控制循环的节奏
		cm.mut.Lock()
		if cm.state != Candidate && cm.state != Follower {
			fmt.Println("当前状态leader，不自循环等待变成candidate")
			cm.mut.Unlock()
			return
		}
		if saveTerm != cm.currentTerm {
			fmt.Println("当前节点的任期和选举前任期不符合,停止自旋")
			cm.mut.Unlock()
			return
		}
		//如果和开始时间的差大于一个周期，代表自旋等于一个周期的时间，此时可以选举
		if elapsed := time.Since(cm.electionResetEvent); elapsed >= timeoutDuration {
			cm.startElection()
			cm.mut.Unlock()
			return
		}
		cm.mut.Unlock()
	}
}

// electionTimeout 会产生一个随机超时时间，这个时间是重置选举的等待时间
// 当当前时间-reset时间>这个随即时间 才能开始请求选举，不然不能选举
func (cm *ConsensusModule) electionTimeout() time.Duration {
	rand.Seed(time.Now().UnixNano())
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

//申请选主的rpc参数
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (cm *ConsensusModule) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	//fmt.Println("被请求")
	cm.mut.Lock()
	defer cm.mut.Unlock()
	if cm.state == Dead {
		cm.consoleLog("当前节点[%d]已经gg了 不参与投票")
		return nil
	}
	cm.consoleLog("请求当前投票内容: %+v [当前任期=%d, 投票情况=%d]", args, cm.currentTerm, cm.voteFor)
	//如果当前请求我投票的节点任期比我大 ， 我就转换成follower
	if args.Term > cm.currentTerm {
		//这里是串行的 要注意
		cm.becomeFollower(args.Term)
	}
	if args.Term == cm.currentTerm && (cm.voteFor == -1 || cm.voteFor == args.CandidateId) {
		reply.VoteGranted = true
		cm.voteFor = args.CandidateId
		cm.electionResetEvent = time.Now() //注意这里，投票完毕之后，就重新开始选举周期时间，避免特去选举
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.consoleLog("节点[%d]投票结束，返回内容：%v", cm.Id, reply)
	return nil
}

//申请选举
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	saveTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.voteFor = cm.Id
	cm.consoleLog("[%d]号节点转换成candidate.当前任期为：%d,当前时间为：%d", cm.Id, saveTerm, time.Now().UnixNano())
	//下面开始向所有的节点都请求他们为自己投票
	var votesReceived int32 = 1 //首先自己投自己一票
	for _, peerId := range cm.peerIds {
		if peerId == cm.Id {
			continue
		}
		go func(peerId int) {
			args := RequestVoteArgs{
				Term:        saveTerm,
				CandidateId: cm.Id,
			}
			var reply RequestVoteReply
			if err := cm.server.call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mut.Lock()
				defer cm.mut.Unlock()
				//fmt.Println("call reply:", reply)
				if cm.state != Candidate {
					cm.consoleLog("当前节点状态为：%v ，所以不继续选举", cm.state)
					return
				}
				if reply.Term > saveTerm {
					cm.consoleLog("当前收到其他节点返回的term[%d], 大于节点请求的任期[%d]", reply.Term, saveTerm)
					cm.becomeFollower(reply.Term)
					return
				} else if reply.Term == saveTerm { //当前reply的任期是其他节点转换成follower之后任期和请求保持一致
					//下面判断在相同任期下是否同意,自己先同意自己
					if reply.VoteGranted {
						vote := int(atomic.AddInt32(&votesReceived, 1))
						if vote*2 > len(cm.peerIds) {
							cm.consoleLog("[%d]号节点，term为：%d,得票[%d],满足成为leader", cm.Id, saveTerm, vote)
							cm.startLeader()
							return
						}
					}
				}
			}
		}(peerId)
	}
	//candidate选举失败： 从新开始
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) becomeFollower(term int) {
	cm.consoleLog("节点[%d]成为follower", cm.Id)
	cm.state = Follower
	cm.currentTerm = term
	cm.voteFor = -1 //未投票过
	cm.electionResetEvent = time.Now()
	go cm.runElectionTimer()
}

func (cm *ConsensusModule) startLeader() {
	cm.consoleLog("节点[%d],当前为Leader", cm.Id)
	cm.state = Leader
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			cm.leaderSendHeartbeats()
			<-ticker.C
			cm.mut.Lock()
			if cm.state != Leader {
				cm.mut.Unlock()
				return
			}
			cm.mut.Unlock()
		}
	}()
}

// 主节点的心跳包
type AppendEntriesArgs struct {
	Term     int
	LeaderId int

	PrevLogIndex int
	PrevLogTerm  int
	//Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	cm.consoleLog("节点：[%d]，发送心跳包", cm.Id)
	cm.mut.Lock()
	saveTerm := cm.currentTerm
	cm.mut.Unlock()
	for _, peerId := range peerIds {
		if peerId == cm.Id {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     saveTerm,
			LeaderId: cm.Id,
		}
		go func(peerId int) {
			var reply AppendEntriesReply
			if err := cm.server.call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mut.Lock()
				defer cm.mut.Unlock()
				if reply.Term > saveTerm {
					fmt.Printf("节点[%d]:当前其他节点回复心跳大于本节点， leader转换成follower\n", cm.Id)
					cm.becomeFollower(reply.Term)
					return
				}
			}

		}(peerId)
	}
}
func (cm *ConsensusModule) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	cm.mut.Lock()
	defer cm.mut.Unlock()
	if cm.state == Dead {
		return fmt.Errorf("当前节点已经死了，不接受心跳")
	}
	if args.Term > cm.currentTerm {
		cm.becomeFollower(args.Term)
	}
	reply.Success = false
	if args.Term == cm.currentTerm {
		if cm.state != Follower {
			cm.becomeFollower(args.Term)
		}
		//重新开始自旋的时间
		cm.electionResetEvent = time.Now()
		reply.Success = true
	}
	reply.Term = cm.currentTerm
	cm.consoleLog("节点[%d],接受到节点[%d]的心跳包,当前心跳任期[%d],节点返回任期[%d]", cm.Id, args.LeaderId, args.Term, cm.currentTerm)
	return nil
}

func (cm *ConsensusModule) consoleLog(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
