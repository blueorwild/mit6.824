package mr

import (
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"io/ioutil"
	"encoding/json"
	"sort"
)

var mapf_ func(string, string) []KeyValue
var reducef_ func(string, []string) string


// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey [] KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	mapf_ = mapf
	reducef_ = reducef

	// 循环处理
	for{
		reply, ok := RequestTask()
		if !ok{
			return
		}

		switch reply.TaskType {
		case Map:
			HandleMap(reply)
		case Reduce:
			HandleReduce(reply)
		case Wait:
			time.Sleep(time.Second)
		case Exit:
			return
		default:
			log.Fatal("未知的任务类型")
		}
	}
}

// 处理Map任务
func HandleMap(reply RequestTaskReply){
	// 读取文件内容 得到中间结果
	file, err := os.Open(reply.FileName)
	if err != nil{
		log.Fatalf("不能打开文件 %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil{
		log.Fatalf("不能读取文件 %v", reply.FileName)
	}
	file.Close()
	mid_res := mapf_(reply.FileName, string(content))

	// 存储中间结果
	// 先创建一堆临时文件对象
	pwd, _ := os.Getwd()      // 获取当前路径
	files := make([] (*os.File), reply.NReduce)
	filenames := make([] string, reply.NReduce)
	for i:=0; i < reply.NReduce; i++{

		tmp_filename := fmt.Sprintf("mr-%v-%v-*", reply.MapTaskNumber, i)
		tmp_file, err := ioutil.TempFile(pwd, tmp_filename)
		if err != nil{
			log.Fatalf("不能创建临时文件 %v\n", tmp_filename)
		}
		files[i] = tmp_file
		filenames[i] = tmp_file.Name()
	}

	// 然后往文件输入内容
	for _, kv := range mid_res{
		reduce_index := ihash(kv.Key) % reply.NReduce
		
		encoder := json.NewEncoder(files[reduce_index])
		err := encoder.Encode(kv)
		if err != nil{
			log.Fatalf("不能encode %v : %v\n", kv, err)
		}
	}

	// 关闭文件
	for i:=0; i < reply.NReduce; i++{
		err := files[i].Close()
		if err != nil{
			log.Fatalf("不能关闭文件 %v : %v \n", files[i].Name(), err)
		}
	}

	// 最后报告任务完成
	ReplyTask(Map, reply.MapTaskNumber, filenames)
}

// 处理Reduce任务
func HandleReduce(reply RequestTaskReply){
	// 读取中间文件内容
	mid_res := [] KeyValue{}
	for i:=0; i < reply.NMap; i++{
		filename := fmt.Sprintf("mr-%d-%d", i, reply.ReduceTaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("不能打开文件 %v : %v \n", file, err)
		}

		decoder := json.NewDecoder(file)
		for{
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				break
			}
			mid_res = append(mid_res, kv)
		}
	}
	sort.Sort(ByKey(mid_res))
	
	// 创建最终输出文件
	filename := fmt.Sprintf("mr-out-%d", reply.ReduceTaskNumber)
	file, err := os.Create(filename)
	if err != nil{
		log.Fatalf("不能创建文件 %v\n", file)
	}

	// 输出
	i :=0
	for i < len(mid_res) {
		j := i + 1
		for j < len(mid_res) && mid_res[j].Key == mid_res[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, mid_res[k].Value)
		}
		output := reducef_(mid_res[i].Key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(file, "%v %v\n", mid_res[i].Key, output)
		i = j
	}
	file.Close()

	// 最后报告任务完成
	ReplyTask(Reduce, reply.ReduceTaskNumber, [] string{filename})
}


// 向master请求任务的rpc调用
func RequestTask()(RequestTaskReply, bool) {
	args := RequestTaskArgs{}   // 请求任务是空参数
	reply := RequestTaskReply{}
	ok := call("Master.GiveTask", &args, &reply)
	return reply, ok
}

// 向master报告任务完成的rpc调用
func ReplyTask(taskType   string,  taskNumber int,  fileNames  []string ) {
	args := DoneTaskArgs{taskType, taskNumber, fileNames}   
	reply :=  DoneTaskReply{}
	ok := call("Master.DoneTask", &args, &reply)
	if !ok{
		log.Println("Call Master.DoneTask Fail")
	}
}

//
// 向 master 发起rpc调用
func call(rpcname string, args interface{}, reply interface{}) bool {
	// 同样 unix的文件句柄模拟
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
