package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"regexp"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		job := Job{}
		CallRetrieveJob(&job)
		switch job.JobType {
		case JOB_TYPE_MAP:
			ExecuteMapJob(&job, mapf)
		case JOB_TYPE_REDUCE:
			ExecuteReduceJob(&job, reducef)
		case JOB_TYPE_WAIT:
			log.Printf("Wait for 3s.")
			time.Sleep(3 * time.Second)
		case JOB_TYPE_DONE:
			log.Printf("All jobs are completed, work node exits.")
			os.Exit(0)
		}
	}

	// ExecuteReduceJob(reducef, 0)
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func ExecuteReduceJob(job *Job, reducef func(string, []string) string) {
	tmp_dir_path := "./tmp/"
	files, err := ioutil.ReadDir(tmp_dir_path)
	if err != nil {
		log.Fatal(err)
	}

	re, err := regexp.Compile(fmt.Sprintf("mr-test-[0-9]+-%d", job.ReduceIndex))

	if err != nil {
		log.Fatal(err)
	}
	intermediate := []KeyValue{}
	for _, f := range files {
		found := re.MatchString(f.Name())
		if found {
			file_path := tmp_dir_path + f.Name()
			log.Printf("Reduce Job: Find tmp file : %s .\n", file_path)
			file, err := os.Open(file_path)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", file_path)
			}
			m_kvs := []KeyValue{}
			dec := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				m_kvs = append(m_kvs, kv)
			}

			// content, err := ioutil.ReadAll(file)
			// if err != nil {
			// 	log.Fatalf("cannot read %v", file_path)
			// 	log.Printf("Length of content is %d", len(content))
			// }
			// lines := strings.Split(string(content), "\n")
			// for mi, l := range lines {
			// 	kv_arr := strings.Split(l, ",")
			// 	if len(kv_arr) == 2 {
			// 		m_kvs = append(m_kvs, KeyValue{strings.Trim(kv_arr[0], " "), strings.Trim(kv_arr[1], " ")})
			// 	} else {
			// 		log.Printf("Unable to parse line %d: %s in %s. \n", mi, l, file_path)
			// 	}
			// }
			intermediate = append(intermediate, m_kvs...)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", job.ReduceIndex)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	CallFinishOneJob(*job)
}

func ExecuteMapJob(job *Job, mapf func(string, string) []KeyValue) {
	kv := CountOneFile(job, mapf)
	buckets := make([]Bucket, job.NReduce)
	for _, e := range kv {
		index := ihash(e.Key) % job.NReduce
		buckets[index].KVs = append(buckets[index].KVs, e)
	}
	tmp_path := "./tmp"
	if _, err := os.Stat(tmp_path); os.IsNotExist(err) {
		if m_err := os.Mkdir(tmp_path, 0700); m_err != nil {
			log.Fatal("Fail to create dir: "+tmp_path, m_err)
		}
		// TODO: handle error
	}

	for i, e := range buckets {
		tmp_file_write(tmp_path, fmt.Sprintf("mr-test-%d-%d", job.JobId, i), e.KVs)
	}

	CallFinishOneJob(*job)
}

//
// Write to local file system
//
func tmp_file_write(dir string, filename string, kvs []KeyValue) {
	path := dir + "/" + filename
	f, err := os.OpenFile(path, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	defer f.Close()
	if err != nil {
		log.Fatal(err)
	}

	// if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
	// 	log.Println("add share lock in no block failed", err)
	// }

	enc := json.NewEncoder(f)
	for _, kv := range kvs {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

	// var build strings.Builder
	// for _, e := range kv {
	// 	build.WriteString(fmt.Sprintf("%s, %s\n", e.Key, e.Value))
	// }
	// content := build.String()
	// if _, err := f.Write([]byte(content)); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
	// 	log.Println("unlock share lock failed", err)
	// }

	// if err := f.Close(); err != nil {
	// 	log.Fatal(err)
	// }
}

// call mapf
func CountOneFile(job *Job, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(job.JobValue)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", job.JobValue)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", job.JobValue)
	}
	kva := mapf(job.JobValue, string(content))
	return kva
}

//
// Retrieve Job from server
//
func CallRetrieveJob(j *Job) error {
	reply := JobReply{}
	args := BaseArgs{}
	ok := call("Coordinator.GetOneJob", &args, &reply)

	if ok {
		// j = &reply.JobDetail
		*j = reply.JobDetail
	} else {
		log.Printf("Call failed!\n")
	}
	return nil
}

//
// Retrieve Job from server
//
func CallFinishOneJob(j Job) error {
	reply := JobReply{}
	args := JobArgs{j}

	ok := call("Coordinator.FinishOneJob", &args, &reply)
	if ok {
		log.Printf("Completed the job %d\n", j.JobId)
	} else {
		log.Printf("Call failed!\n")
	}
	return nil
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
