package raft

//
// Raft 向 tester 提供的公开API，包括以下：
// Make：创建一个新的Raft Server
// Start：对新的log entry 启动 agreement
// GetState：向一个Raft 节点询问 当前term，以及它是否认为自己是leader

import (
	"sync"
	"sync/atomic"
	"time"
	"math/rand"
	"bytes"

	"../labgob"
	"../labrpc"
)

const (
	Follower = 0
	Leader = 1
	Candidate = 2
)

// ApplyMsg
// 每次将一个新entry提交到log中时，每个raft节点都应该向同一服务器中的服务(或tester)
// 发送一个ApplyMsg。将CommandValid设置为true，表示ApplyMsg包含一个新提交的日志条目。

// 在实验3中，会发送其他类型的消息(如，快照);此时，可向ApplyMsg添加字段，
// 但需要将CommandValid设置为false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct{
	Command interface{}
	Term int
}

// 实现单一Raft节点的go 对象
type Raft struct {
	mu        sync.Mutex          			 // 用于互斥访问此节点的state
	peers     []*labrpc.ClientEnd   // RPC的所有对等节点
	persister *Persister          // 该节点的persisted状态
	me        int                             // 该节点在peers[]中的索引
	dead      int32                      // set by Kill()

	// 2A
	currentTerm int   // 节点当前任期
	state int  // 节点当前状态
	voteReceived int // 当前节点收到的投票总数
	votedFor int // 投票给某个CandidateID

	minElectionTimeout int   // 每个节点的选举超时时间是随机的，这里确定范围  /ms
	maxElectionTimeout int
	electionTimer *time.Timer     // 选举的定时器
	heartbeatInterval int // 心跳时间间隔 /ms
	heartbeatTimer *time.Timer  // 心跳的定时器

	// 2B
	commitIndex int  // 节点已确定提交的最大Log索引号
	lastApplied int  // 节点已应用到状态机的最大Log索引号
	nextIndex []int  // leader专用，要发送到其它server的下一个日志条目的索引（初始化为leader的最后一个日志索引+1）
	matchIndex []int // leader专用，已知要在其它server复制的最高日志条目索引（初始化为0）
	log []LogEntry // 日志条目，每个条目包含用于状态机的command，以及leader接收到条目时的term(第一个索引为1)
    
    applyCh chan ApplyMsg  // 用于ApplyMsg的接收
}

// 获取节点的部分状态
func (rf *Raft) GetState() (int, bool) {
    var term int
    var isleader bool
    // Your code here (2A).
    rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
    isleader = rf.state == Leader
    return term, isleader
}

// 保存raft的持久状态到稳定存储以便重启后能获取
func (rf *Raft) persist() {
	// 2C. Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


// 恢复之前的持久状态
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // 没有任何状态的引导（bootstrap）
		return
	}
	// 2C.Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || 
			d.Decode(&votedFor) != nil  || d.Decode(&log) != nil{
		DPrintf("解码raft状态失败")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

// 投票RPC的请求回复结构体
type RequestVoteArgs struct {
	// 2A
	Term int  // 投票者的任期
	CandidateId int  // 投票者的ID（请求投票都是给自己投，就看谁投的快，任期高）

	// 2B
	LastLogIndex int // leader最大日志的索引号
	LastLogTerm int  // LastLogIndex 条目的任期号
}
type RequestVoteReply struct {
	Term  int// 回复者的任期
	VoteGranted bool  // 是否同意投票者，若任期比自己高且还未同意其它人则同意
}

// 发送请求投票的RPC给其它节点
// server即rf.peers[] 中的某个目标节点
// labrpc包模拟了一个有损耗的网络，在其中服务器可能是不可到达的，
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 处理请求投票的RPC
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // 改动需要持久化

    // 任期小于自己的，或者任期等于自己但自己已投票，返回false
    if args.Term < rf.currentTerm ||
        (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    // 任期大于自己，直接同意，转成Follower
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.updateStateTo(Follower)
    }

    // 任期比完了还要比日志
    // 如果请求者的最新日志任期比自己最新日志任期小 或者 任期相等但日志索引小了， 返回false
    lastLogIndex := len(rf.log) - 1
    if args.LastLogTerm < rf.log[lastLogIndex].Term ||
        (args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < lastLogIndex) {
        reply.Term = rf.currentTerm
        reply.VoteGranted = false
        return
    }

    // 到这就是同意投票了，注意重置选举定时器
    rf.votedFor = args.CandidateId
    reply.Term = rf.currentTerm
    reply.VoteGranted = true
    rf.electionTimer.Reset(rf.randElectionTimeout())
}

// Append entry的请求回复结构体
type AppendEntryArgs struct {
	// 2A
	Term int   // leader的任期
	LeaderID int  

	// 2B
	Entries  []LogEntry // 发送的日志条目（表示心跳时为空）
	PrevLogIndex int // 发送的新的日志条目紧邻的之前的最后一条日志索引值（为了日志的一致性）
	PrevLogTerm int  // PrevLogIndex条目的任期号（为了日志的一致性）
	LeaderCommit int // leader已经提交的日志索引值
}
type AppendEntryReply struct {
	Term  int // 回复者的任期
	Success bool  

	// 2C
	// 如果需要的话，可以对协议进行优化，以减少被拒绝的AppendEntries rpc的数量。
	// 在之前的实验中，当leader发出的appendebtries rpc因为相同任期条目有冲突被拒绝时，
	// leader是递减条目索引，这样一个个去尝试。
	// 而现在，follower在拒绝请求时，可以包含自己在冲突条目的任期，以及自己在该任期
	// 的第一个条目索引号，这样的话leader就可以直接把nextindex减到那个任期的第一个
	// 条目而不用一条条尝试。可能会跳过正确的条目? 不影响正确性。
    // 如此一来，一个任期内的冲突条目只需要一次rpc即可，而不是每个日志条目一个rpc
	ConflictTerm int
	ConflictIndex int
}

// 类似的，发送append entry给其它节点
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// 处理append entry的RPC, 包括心跳和日志条目
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
    rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()   // 改动需要持久化
    reply.Success = true

    // 任期小于自己直接放回false
    if args.Term < rf.currentTerm {
        reply.Success = false
        reply.Term = rf.currentTerm
        return
    }
    // 只要任期不小于自己，都保持跟随状态，选举定时器重置
    rf.electionTimer.Reset(rf.randElectionTimeout())

    // 任期大于自己转成Follower
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.updateStateTo(Follower)
    }

    // 然后比较日志
    // 如果leader打算复制的最老日志比自己的最新日志索引还要大，表示有缺失，返回false
    lastLogIndex := len(rf.log) - 1
    if lastLogIndex < args.PrevLogIndex {
        reply.Success = false
		reply.Term = rf.currentTerm
		// 2C
		reply.ConflictIndex = len(rf.log)
        reply.ConflictTerm = -1
        return
    }

    // 如果leader打算复制的最老日志的任期和自己的相同日志索引的任期不同，表示有冲突，返回false
    if rf.log[(args.PrevLogIndex)].Term != args.PrevLogTerm {
        reply.Success = false
		reply.Term = rf.currentTerm
		
		// 2C
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
        conflictIndex := args.PrevLogIndex
        for rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
			if conflictIndex == 0{
				break
			}
        }
        reply.ConflictIndex = conflictIndex
        return
    }

    // 到这就可以覆盖追加新日志了，为了简便，直接从leader打算复制的最老日志开始覆盖了
    rf.log = append(rf.log[:args.PrevLogIndex + 1], args.Entries...)

    // 最后，如果leader的已提交索引大于自己的，自己跟着提交
    if args.LeaderCommit > rf.commitIndex {
        rf.setCommitIndex(Min(args.LeaderCommit, len(rf.log)-1))
    }
    reply.Success = true
}

// Start()的功能是将接收到的客户端命令追加到自己的本地log，
// 然后给其他所有peers并行发送AppendEntries RPC来迫使其他peer也同意领导者日志的内容，
// 在收到大多数peers的已追加该命令到log的肯定回复后，若该entry的任期等于leader的当前任期，
// 则leader将该entry标记为已提交的(committed)，提升commitIndex到该entry所在的index，
// 并发送ApplyMsg消息到ApplyCh，相当于应用该entry的命令到状态机。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// 2B
	rf.mu.Lock()
    defer rf.mu.Unlock()
    term = rf.currentTerm
	isLeader = (rf.state == Leader)
	if isLeader{
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})   // golang 的 拼接
		rf.persist()  // 改动需持久化
		index = len(rf.log) - 1
		rf.matchIndex[rf.me] = index
		rf.nextIndex[rf.me] = index + 1
	}

	return index, term, isLeader
}

// 测试器不会在每次测试后停止由Raft创建的goroutines，但是它会调用Kill()方法。
// 问题是，长时间运行的goroutine会使用内存并消耗CPU时间，可能会导致以后的测试失败并产生令人困惑的调试输出。
// 任何具有长时间运行循环的goroutine都应该调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 需要循环处理的， 选举、心跳
func(rf* Raft) startLoop(){
	// 选举定时器到了，Follower转成Candidate, Candidate开始新的选举
	// 心跳定时器到了，Leader重新发心跳
	for{
		select{
		case <- rf.electionTimer.C:
			rf.mu.Lock()
			switch rf.state{
			case Follower:
				rf.updateStateTo(Candidate)
			case Candidate:
				rf.startElection()
			}
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
            if rf.state == Leader{
                rf.broadcastAppendEntry()
                rf.heartbeatTimer.Reset(time.Duration(int64(rf.heartbeatInterval)) * time.Millisecond)
            }
            rf.mu.Unlock()
		}
	}
}

// 创建一个Raft节点。所有Raft节点的端口都在peer[]中。
// 此节点的端口是peer [me]。所有节点的peer[]数组具有相同的顺序。
// persister是此节点保存其持久状态的地方，初始化保存最近保存的状态(如果有的话)。
// applyCh是tester期望Raft发送ApplyMsg消息的通道。
// Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// 2A
	rf.currentTerm = 0
	rf.state = Follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	rf.minElectionTimeout = 300
	rf.maxElectionTimeout = 400
	rf.heartbeatInterval = 100
	rf.electionTimer = time.NewTimer(rf.randElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(time.Duration(int64(rf.heartbeatInterval)) * time.Millisecond)

	// 2B
    rf.log = make([]LogEntry, 1)
	rf.nextIndex = make([]int, len(rf.peers))	
    rf.matchIndex = make([]int, len(rf.peers))
    

	// 2C
	// 重启后从持久化状态进行初始化
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	for i := range rf.nextIndex {
        rf.nextIndex[i] = len(rf.log)
	}
	
	// 开始循环
	go rf.startLoop()

	DPrintf("初始化节点 %v 成功\n", me)
	return rf
}

// 生成随机选举超时时间
func (rf* Raft) randElectionTimeout() time.Duration{
	r := rand.New(rand.NewSource(time.Now().UnixNano()))  // seed
	randTime := r.Intn((rf.maxElectionTimeout - rf.minElectionTimeout)) + rf.minElectionTimeout
    return time.Millisecond * time.Duration(int64(randTime))
}

// 节点状态转换（调用者需加锁）
func (rf *Raft) updateStateTo(state int){
	if rf.state == state{
		return
	}
	rf.state = state
	switch state{
	case Follower:
		// 有可能是leader转的，所以停止心跳定时器，重启选举定时器
		// 另外清理投票信息
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(rf.randElectionTimeout())
		rf.votedFor = -1

	case Candidate:
		// 成功候选者后立即开始选举，因为不可能是leader转的，所以无需停止心跳定时器
		rf.startElection()

    case Leader:
        // 先处理日志索引
        for i := range rf.nextIndex {
            rf.nextIndex[i] = (len(rf.log))
        }
        for i := range rf.matchIndex {
            rf.matchIndex[i] = 0
        }
        // 停止选举定时器，发送心跳
        rf.electionTimer.Stop()
		rf.broadcastAppendEntry()
		rf.heartbeatTimer.Reset(time.Duration(int64(rf.heartbeatInterval)) * time.Millisecond)
	}
}

// leader节点 广播心跳和普通日志
func (rf *Raft) broadcastAppendEntry(){
	// 遍历peers，并行发出append entry并处理回复
    for i, _ := range rf.peers {
        if i != rf.me {
			go func(server int) {
                rf.mu.Lock()
				if rf.state != Leader {
                    rf.mu.Unlock()
					return
				}
				
                prevLogIndex := rf.nextIndex[server] - 1
				// 深拷贝
				logs :=  make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
				copy(logs, rf.log[(prevLogIndex+1):])
				args := AppendEntryArgs{
					Term: rf.currentTerm, 
					LeaderID: rf.me,
			
					Entries: logs,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[prevLogIndex].Term,
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntryReply{}
				
				if rf.sendAppendEntry(server, &args, &reply) {
					rf.mu.Lock()
					if rf.state != Leader {
                        rf.mu.Unlock()
                        return
                    }
                    if reply.Success {
						// 如果成功:更新追随者的nextIndex和matchIndex
                        rf.matchIndex[server] = prevLogIndex + len(logs)
                        rf.nextIndex[server] = rf.matchIndex[server] + 1
                        // 如果收到超过一半的成功回复，leader就可以提交了
                        for N := len(rf.log) - 1; N > rf.commitIndex; N--{
                            count := 0
                            for _, matchIndex := range rf.matchIndex{
                                if matchIndex >= N{
                                    count += 1
                                }
                            }

                            if count > (len(rf.peers) / 2){
                                rf.setCommitIndex(N)
                                break
                            }
                        }
                    } else {
                        if reply.Term > rf.currentTerm {
                            rf.currentTerm = reply.Term
							rf.updateStateTo(Follower)
							rf.persist()  // 改动需要持久化
                        }else{
                            // 如果附件条目由于日志不一致而失败, 递减nextIndex,下一次心跳重试
							// rf.nextIndex[server] = args.PrevLogIndex - 1;
							// 2C  优化前推
							// 如果是第一个term冲突，直接覆盖所有条目就好
							// 否则找到与冲突条目相同任期的第一个条目？，覆盖之后的条目
							rf.nextIndex[server] = reply.ConflictIndex
							if reply.ConflictTerm != -1{
								for i := args.PrevLogIndex; i >= 1; i--{
									if rf.log[i-1].Term == reply.ConflictTerm{
										rf.nextIndex[server] = i 
										break
									}
								}
							}
                        }
                    }
                    rf.mu.Unlock()
				}
			}(i)
		}
    }
}

// Candidate节点开始一轮新的选举 (广播投票请求)
func(rf *Raft) startElection(){
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()  // 改动需要持久化
	rf.voteReceived = 1
    rf.electionTimer.Reset(rf.randElectionTimeout()) 
    
	// 遍历peers，并行发起请求投票并处理回复
	for i := range rf.peers {
		if i != rf.me {
			go func(server int){
                rf.mu.Lock()
                lastLogIndex := len(rf.log) - 1
                args := RequestVoteArgs{
                    Term:  rf.currentTerm,
                    CandidateId: rf.me,
                    LastLogIndex: lastLogIndex,
                    LastLogTerm: rf.log[lastLogIndex].Term,
                }
                rf.mu.Unlock()

				reply := RequestVoteReply{}
				if rf.sendRequestVote(server, &args, &reply){
                    rf.mu.Lock()
                    defer rf.mu.Unlock()
                    // 处理回复
                    if reply.Term > rf.currentTerm {
                        rf.currentTerm = reply.Term
						rf.updateStateTo(Follower)
						rf.persist() // 改动需要持久化
                    }
					if reply.VoteGranted && rf.state == Candidate{
                        rf.voteReceived++
                        // 收到投票超过半数，转为leader
						if rf.voteReceived > len(rf.peers)/2{
							rf.updateStateTo(Leader)
							// leader不需要持久化，重启之后肯定不应该是leader了 
						}
					}
				}
			}(i)
		}
	}
}

// 设置节点的commitIndex，如果比lastApplied大，需要把中间的日志条目提交到状态机 
func (rf *Raft) setCommitIndex(commitIndex int) {
    rf.commitIndex = commitIndex
    if rf.commitIndex > rf.lastApplied {
        entriesToApply := append([]LogEntry{}, rf.log[(rf.lastApplied+1):(rf.commitIndex+1)]...)

        go func(startIdx int, entries []LogEntry) {
            for idx, entry := range entries {
                msg := ApplyMsg{
					CommandValid: true,
					Command: entry.Command,
					CommandIndex: startIdx + idx,
				}
				rf.applyCh <- msg
				// 更新lastApplied
                rf.mu.Lock()
                if rf.lastApplied < msg.CommandIndex {
                    rf.lastApplied = msg.CommandIndex
                }
                rf.mu.Unlock()
            }
        }(rf.lastApplied+1, entriesToApply)
    }
}