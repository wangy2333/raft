package ConsensusModule

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"raft/part3/server/storage"
	"strconv"
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

type CommitEntry struct {
	//客户端的命令
	Command interface{}
	//当前指令写入到log中的index
	Index int
	//当前指令存入log中的term
	Term int
}

//日志的基本单元
type LogEntry struct {
	Command interface{}
	Term    int
}

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
	//本地日志
	log []LogEntry
	///newCommitReadyChan是CM内部使用的一个通道，用来通知在commit channel上有新条目可以发生给客户端。
	newCommitReadyChan chan struct{}
	//在新建ConsensusModule时会接收一个commit channel作为参数——CM可以使用该通道向调用方发送已提交的指令
	//commitChan chan<- CommitEntry。
	commitChan chan CommitEntry
	//最后一个commit的index
	commitIndex int
	//最后一个applied的index
	lastApplied int
	//cm模块状态为leader的时候使用的不稳定模块
	//nextIndex是leader记录了当前follower的日志最后一个可能合理插入的位置，需要等follower确认
	nextIndex map[int]int
	//matchIndex是leader记录的当前follower同步日志之后的长度，也就是同步后可插入的index
	matchIndex map[int]int
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
	//存储模块
	storage storage.Storage
}

func NewConsensusModule(serverId int, peerIds []int, storage storage.Storage, ready <-chan struct{}, srv *server) *ConsensusModule {
	cm := &ConsensusModule{}
	cm.Id = serverId
	cm.voteFor = -1 //初始-1代表没有投票过
	cm.currentTerm = 0
	cm.state = Follower
	cm.peerIds = peerIds
	cm.server = srv
	cm.storage = storage
	cm.commitIndex = -1
	cm.lastApplied = -1
	cm.commitChan = make(chan CommitEntry, 16)
	//newCommitReadyChan是负责通知的所以无缓冲
	cm.newCommitReadyChan = make(chan struct{})
	cm.nextIndex = make(map[int]int)
	cm.matchIndex = make(map[int]int)
	if cm.storage.HasDate() {
		cm.restoreFromStorage(cm.storage)
	}
	go func() {
		//fmt.Println("等待信号")
		<-ready
		cm.mut.Lock()
		cm.electionResetEvent = time.Now()
		cm.mut.Unlock()
		cm.runElectionTimer()
	}()
	go cm.commitChanSender()
	go cm.commitCommandPersistent()
	//我认为应该在这个地方起一个goroutine 然后接受commitChan的提交的log 然后将其写入map或者是文件当中
	//fmt.Println("启动服务的时候，信息", cm.voteFor, cm.currentTerm, cm.log)
	return cm
}
func (cm *ConsensusModule) Submit(command interface{}) (bool, int) {
	cm.mut.Lock()
	defer cm.mut.Unlock()

	//cm.consoleLog("Submit received by %v: %v", cm.state, command)
	if cm.state == Leader {
		cm.log = append(cm.log, LogEntry{Command: command, Term: cm.currentTerm})
		//cm.consoleLog("... log=%v", cm.log)
		cm.persistToStorage()
		return true, cm.Id
	}
	return false, -1
}

func (cm *ConsensusModule) runElectionTimer() {
	//fmt.Printf("[%d]号节点 开始选举\n", cm.Id)
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
			//fmt.Println("当前状态leader，不自循环等待变成candidate")
			cm.mut.Unlock()
			return
		}
		if saveTerm != cm.currentTerm {
			//fmt.Println("当前节点的任期和选举前任期不符合,停止自旋")
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
		cm.consoleLog("已经gg了 不参与投票")
		return nil
	}
	//获取当前模块的日志的最后一个日志的index和term
	lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
	//cm.consoleLog("请求当前投票内容: %+v [当前任期=%d, 投票情况=%d]", args, cm.currentTerm, cm.voteFor)
	//如果当前请求我投票的节点任期比我大 ， 我就转换成follower
	if args.Term > cm.currentTerm {
		//这里是串行的 要注意
		cm.becomeFollower(args.Term)
	}
	if args.Term == cm.currentTerm &&
		(cm.voteFor == -1 || cm.voteFor == args.CandidateId) &&
		//新加的同意竞选的条件，限制一下，如果当前请求的最后log的任期大于自身且（当前日志任期相同或者日志的索引大于自己）说明请求的比自己存的log多，
		//这才同意，如果申请的节点的本地log比自己还少，是不能让他成为leader的。
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm || args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		cm.voteFor = args.CandidateId
		cm.electionResetEvent = time.Now() //注意这里，投票完毕之后，就重新开始选举周期时间，避免特去选举
	} else {
		reply.VoteGranted = false
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	//cm.consoleLog("投票结束，返回内容：%v", reply)
	return nil
}

//申请选举
func (cm *ConsensusModule) startElection() {
	cm.state = Candidate
	cm.currentTerm += 1
	saveTerm := cm.currentTerm
	cm.electionResetEvent = time.Now()
	cm.voteFor = cm.Id
	//cm.consoleLog("转换成candidate.当前任期为：%d,当前时间为：%d", saveTerm, time.Now().UnixNano())
	//下面开始向所有的节点都请求他们为自己投票
	var votesReceived int32 = 1 //首先自己投自己一票
	for _, peerId := range cm.peerIds {
		if peerId == cm.Id {
			continue
		}
		go func(peerId int) {
			lastLogIndex, lastLogTerm := cm.lastLogIndexAndTerm()
			args := RequestVoteArgs{
				Term:         saveTerm,
				CandidateId:  cm.Id,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply RequestVoteReply
			if err := cm.server.call(peerId, "ConsensusModule.RequestVote", args, &reply); err == nil {
				cm.mut.Lock()
				defer cm.mut.Unlock()
				//fmt.Println("call reply:", reply)
				if cm.state != Candidate {
					cm.consoleLog("当前节点状态为：%v ，所以不继续选举", cm.state.string())
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
							//cm.consoleLog("term为：%d,得票[%d],满足成为leader", saveTerm, vote)
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
	//cm.consoleLog("成为follower")
	cm.consoleLog("becomes Follower with term=%d;", term)
	cm.state = Follower
	cm.currentTerm = term
	cm.voteFor = -1
	cm.electionResetEvent = time.Now()

	go cm.runElectionTimer()
}
func (cm *ConsensusModule) startLeader() {
	cm.consoleLog("当前为Leader,任期为%d,当前的log为：%v", cm.currentTerm, cm.log)
	cm.state = Leader
	for _, peerId := range cm.peerIds {
		cm.nextIndex[peerId] = len(cm.log)
		cm.matchIndex[peerId] = -1
	}
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
	Term     int //当前的leader任期
	LeaderId int //当前leader的id

	PrevLogIndex int        //即将写新日志前一条日志的索引
	PrevLogTerm  int        //前一条日志的任期
	Entries      []LogEntry //要存储的日志条目
	LeaderCommit int        //leader的提交索引
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (cm *ConsensusModule) leaderSendHeartbeats() {
	//cm.consoleLog("节点：[%d]，发送心跳包", cm.Id)
	cm.mut.Lock()
	saveTerm := cm.currentTerm
	cm.mut.Unlock()
	for _, peerId := range peerIds {
		if peerId == cm.Id {
			continue
		}
		go func(peerId int) {
			cm.mut.Lock()
			//leader记录的不同节点的最后一个可插入位置
			ni := cm.nextIndex[peerId]
			//当前的follower的可插入位置的前一个log的index
			pervLogIndex := ni - 1
			//当前的follower的可插入位置的前一个log的term
			pervLogTerm := -1
			//如果当前pervLogIndex 是存在的 那么term就更新而不是初始状态
			if pervLogIndex >= 0 {
				pervLogTerm = cm.log[pervLogIndex].Term
			}
			//随着心跳包传过去的日志应该是当前本地log的[ni,~]
			entries := cm.log[ni:]
			args := &AppendEntriesArgs{
				Term:         saveTerm,
				LeaderId:     cm.Id,
				PrevLogIndex: pervLogIndex,   //即将写新日志前一条日志的索引
				PrevLogTerm:  pervLogTerm,    //前一条日志的任期
				Entries:      entries,        //要存储的日志条目
				LeaderCommit: cm.commitIndex, //leader的提交索
			}
			cm.mut.Unlock()
			var reply AppendEntriesReply
			//cm.consoleLog("节点在心跳包中发送的args:%v,当前ni为：%d", args, ni)
			if err := cm.server.call(peerId, "ConsensusModule.AppendEntries", args, &reply); err == nil {
				cm.mut.Lock()
				defer cm.mut.Unlock()
				if reply.Term > saveTerm {
					//fmt.Printf("节点[%d]:当前其他节点回复心跳大于本节点， leader转换成follower\n", cm.Id)
					cm.becomeFollower(reply.Term)
					return
				}
				if cm.state == Leader && saveTerm == reply.Term {
					if reply.Success {
						cm.nextIndex[peerId] = ni + len(entries)
						cm.matchIndex[peerId] = cm.nextIndex[peerId] - 1
						//cm.consoleLog("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v", peerId, cm.nextIndex, cm.matchIndex)
						saveCommitIndex := cm.commitIndex
						for i := cm.commitIndex + 1; i < len(cm.log); i++ {
							if cm.log[i].Term == cm.currentTerm {
								matchCount := 1
								for _, peerId := range cm.peerIds {
									if cm.matchIndex[peerId] >= i {
										matchCount++
									}
								}
								if matchCount*2 > len(cm.peerIds) {
									cm.commitIndex = i //存的是最后一个满足多数派的同一个term下的index
								}
							}
						}
						if cm.commitIndex != saveCommitIndex {
							//cm.consoleLog("当前leader持久化log")
							//发送一个消息 告诉可以 更新呢log了
							cm.newCommitReadyChan <- struct{}{}
						}
					} else { //这里就保证了 主节点的那些从节点 如果大于主节点的日志是没有意义的， 最多就是同log索引同任期的最后一个index有意义，其他都会被覆盖掉
						//而这里减1 就是说明可能当前不够需要往前就开始覆盖
						cm.nextIndex[peerId] = ni - 1
						cm.consoleLog("AppendEntries reply from %d no success: nextIndex := %d", peerId, ni-1)
					}
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
		if args.PrevLogIndex == -1 || (args.PrevLogIndex < len(cm.log) && args.PrevLogTerm == cm.log[args.PrevLogIndex].Term) {
			reply.Success = true
			//根据leader日志情况， 插入更新自己的日志的index
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := 0
			for {
				if logInsertIndex >= len(cm.log) || newEntriesIndex >= len(args.Entries) {
					break
				}
				if cm.log[logInsertIndex].Term != args.Entries[newEntriesIndex].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex < len(args.Entries) {
				//cm.consoleLog("... inserting entries %v from index %d", args.Entries[newEntriesIndex:], logInsertIndex)
				cm.log = append(cm.log[:logInsertIndex], args.Entries[newEntriesIndex:]...)
				//cm.consoleLog("... log is now: %v", cm.log)
			}
			if args.LeaderCommit > cm.commitIndex {
				cm.commitIndex = cm.intMin(args.LeaderCommit, len(cm.log)-1)
				cm.consoleLog("从节点发现主的提交大于自己 ，就会持久化")
				cm.newCommitReadyChan <- struct{}{}
			}
		}
	}
	reply.Term = cm.currentTerm
	cm.persistToStorage()
	//cm.consoleLog("节点[%d],接受到节点[%d]的心跳包,当前心跳任期[%d],节点返回任期[%d]", cm.Id, args.LeaderId, args.Term, cm.currentTerm)
	return nil
}

func (cm *ConsensusModule) consoleLog(format string, args ...interface{}) {
	fmt.Printf("["+strconv.Itoa(cm.Id)+"]"+":"+format+"\n", args...)
}
func (cm *ConsensusModule) lastLogIndexAndTerm() (int, int) {
	if len(cm.log) > 0 {
		lastLogIndex := len(cm.log) - 1
		return lastLogIndex, cm.log[lastLogIndex].Term
	} else {
		return -1, -1
	}
}
func (cm *ConsensusModule) intMin(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (cm *ConsensusModule) commitChanSender() {
	for range cm.newCommitReadyChan {
		cm.mut.Lock()
		saveTerm := cm.currentTerm
		saveLastApplied := cm.lastApplied
		var entries []LogEntry
		//commit是我需要提交到log的index 而lastApplied则是之前提交的长度，由此找到从上次到这次准备提交的区间
		if cm.commitIndex > cm.lastApplied {
			entries = cm.log[cm.lastApplied+1 : cm.commitIndex+1]
			cm.lastApplied = cm.commitIndex
		}
		cm.mut.Unlock()
		//最后逐步将log中的command都输入到commitChan当中
		for i, entriy := range entries {
			cm.commitChan <- CommitEntry{
				Command: entriy.Command,
				Index:   saveLastApplied + 1 + i,
				Term:    saveTerm,
			}
		}
	}
	cm.consoleLog("commitChanSender done ")
}
func (cm *ConsensusModule) commitCommandPersistent() {
	for command := range cm.commitChan {
		fmt.Println("写入文件", command)
	}
}

//将存储起来的值重新给cm的几个主要的参数，主要是voteFor term log
func (cm *ConsensusModule) restoreFromStorage(s storage.Storage) {
	if voteFor, found := s.Get("voteForDate"); found {
		decoder := gob.NewDecoder(bytes.NewBuffer(voteFor))
		if err := decoder.Decode(cm.voteFor); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage")
	}
	if currentTerm, found := s.Get("TermDate"); found {
		decoder := gob.NewDecoder(bytes.NewBuffer(currentTerm))
		if err := decoder.Decode(cm.currentTerm); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("currentTerm not found in storage ")
	}
	if Log, found := s.Get("logDate"); found {
		d := gob.NewDecoder(bytes.NewBuffer(Log))
		if err := d.Decode(cm.log); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Fatal("log not found in storage")
	}
}

func (cm *ConsensusModule) persistToStorage() {
	var voteForDate bytes.Buffer
	if err := gob.NewEncoder(&voteForDate).Encode(cm.voteFor); err != nil {
		log.Fatal(err)
	} else {
		cm.storage.Set("voteForDate", voteForDate.Bytes())
	}

	var TermDate bytes.Buffer
	if err := gob.NewEncoder(&TermDate).Encode(cm.currentTerm); err != nil {
		log.Fatal(err)
	} else {
		cm.storage.Set("TermDate", TermDate.Bytes())
	}
	var logDate bytes.Buffer
	if err := gob.NewEncoder(&logDate).Encode(cm.log); err != nil {
		log.Fatal(err)
	} else {
		cm.storage.Set("logDate", logDate.Bytes())
	}
}
