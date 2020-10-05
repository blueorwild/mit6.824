package mr

import (
	"fmt"
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"sync"
	"time"
)

// 任务状态
const(
	Init = 0
	Doing = 1
	Finish = 2
)

type Task struct{
	TaskType string
	State int
	StartTime time.Time    // 要处理超时任务

	// 用于Map任务
	FileName string
}

type Master struct {
	mutex sync.Mutex          // 互斥访问master内容的锁

	MapDone bool                 // 所有 Map 任务是否已完成
	ReduceDone bool           // 所有 Reduce 任务是否已完成
	MapTaskNum int             // Map 任务数量，其实就是输入文件数量
	ReduceTaskNum int       // Reduce 任务数量，其实就是输出文件的数量
	MapTasks []Task               // Map 任务信息
	ReduceTasks []Task        // Reduce 任务信息
}

// 请求颁发任务
func (m *Master) GiveTask(args *RequestTaskArgs,  reply *RequestTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()   // 函数结束自动释放

	if m.MapDone && m.ReduceDone{
		reply.TaskType = Exit
		return nil
	}
	
	// 颁发Map任务
	if !m.MapDone{
		unFinish := false
		for i:=0; i < m.MapTaskNum; i++{
			if m.MapTasks[i].State != Init{
				if m.MapTasks[i].State == Doing{
					unFinish = true
				}
				continue
			}
			
			// 填充reply
			reply.TaskType = Map
			reply.MapTaskNumber = i
			reply.FileName = m.MapTasks[i].FileName
			reply.NReduce = m.ReduceTaskNum

			// 更新任务状态
			m.MapTasks[i].StartTime = time.Now()
			m.MapTasks[i].State = Doing

			return nil
		}

		if unFinish{
			reply.TaskType = Wait
			return nil
		}

		m.MapDone = true
	}

	// 到这里说明Map任务做完了 该发布Reduce任务了
	unFinish := false
	for i:=0; i < m.ReduceTaskNum; i++{
		if m.ReduceTasks[i].State != Init{
			if m.ReduceTasks[i].State == Doing{
				unFinish = true
			}
			continue
		}
		
		// 填充reply
		reply.TaskType = Reduce
		reply.ReduceTaskNumber = i
		reply.NMap = m.MapTaskNum

		// 更新任务状态
		m.ReduceTasks[i].StartTime = time.Now()
		m.ReduceTasks[i].State = Doing

		return nil
	}

	if unFinish{
		reply.TaskType = Wait
		return nil
	}

	m.ReduceDone = true
	reply.TaskType = Exit
	return nil
}

// 报告完成任务
func (m *Master) DoneTask(args *DoneTaskArgs,  reply *DoneTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()   // 函数结束自动释放
	
	// 就两种 Map 和 Reduce，更新状态就行了
	if args.TaskType == Map{
		m.MapTasks[args.TaskNumber].State = Finish

		// 然后确定中间文件的名字
		for i := 0; i < m.ReduceTaskNum ; i++ {
			name := fmt.Sprintf("mr-%v-%v", args.TaskNumber, i)
			fmt.Printf("hehe %v\n", len(args.FileNames))
			os.Rename(args.FileNames[i], name)
		}
	}else{
		m.ReduceTasks[args.TaskNumber].State = Finish
		// 确定输出文件的名字
		name := fmt.Sprintf("mr-out-%v", args.TaskNumber)
		os.Rename(args.FileNames[0], name)
	}

	return nil
}

// 开启协程监听来自worker的调用
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	// 本来应该是监听ip+端口，这里在一台机器上，以unix的文件句柄模拟
	//l, e := net.Listen("tcp", ":1234")  
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go 会周期性的调用这个函数以判断整个MapReduce任务是否已完成
func (m *Master) Done() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()   // 函数结束自动释放

	return m.MapDone && m.ReduceDone
}

//
// 创建一个master
// main/mrmaster.go calls this function.
// nReduce 由用户指定，即ReduceTask的数量，代表最终输出文件的个数
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.MapDone = false
	m.ReduceDone = false
	m.MapTaskNum = len(files)
	m.ReduceTaskNum = nReduce

	m.MapTasks = make([]Task, m.MapTaskNum)
	for i := 0; i < m.MapTaskNum; i++ {
		m.MapTasks[i] = Task{
			TaskType: Map,
			State: Init,
			FileName: files[i],
		}
	}

	m.ReduceTasks = make([]Task, m.ReduceTaskNum)
	for i := 0; i < m.ReduceTaskNum; i++ {
		m.ReduceTasks[i] = Task{
			TaskType: Reduce,
			State: Init,
		}
	}

	m.server()
	return &m
}
