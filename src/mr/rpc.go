package mr

//
// 用于rpc请求和返回的参数
// 这里主要定义worker向master请求任务和报告任务完成的rpc的参数
// 

import "os"
import "strconv"

// 任务类型
const (
	Map = "Map"
	Reduce = "Reduce"
	Wait = "Wait"   // 暂时无任务
	Exit = "Exit"      // 所有任务已完成，可以退出
)

// 请求任务参数（worker 向 master 请求任务）
type RequestTaskArgs struct{
}

// 请求任务回复（master 向 worker 颁发任务）
type RequestTaskReply struct{
	TaskType string

	// 用于map任务
	MapTaskNumber int
	FileName string
	NReduce int

	// 用于reduce任务
	ReduceTaskNumber int
	NMap int
}

// 报告任务完成参数（worker 向 master 报告任务完成）
type DoneTaskArgs struct{
	TaskType   string
	TaskNumber int
	FileNames  []string    // 若是reduce任务该数组仅有一个元素
}

// 报告任务完成回复（master 向 worker 回复）
type DoneTaskReply struct{
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
