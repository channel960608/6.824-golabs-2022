package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	JOB_STATUS_CREATED   string = "JOB_STATUS_CREATED"
	JOB_STATUS_RETRIEVED string = "JOB_STATUS_CREATED"
	JOB_STATUS_COMPLETED string = "JOB_STATUS_COMPLETED"

	STAGE_INITIAL        string = "STAGE_INITIAL"        // initial status
	STAGE_MAP_STARTED    string = "STAGE_MAP_STARTED"    // map started
	STAGE_MAP_DONE       string = "STAGE_MAP_DONE"       // map has been done, ready for reduce
	STAGE_REDUCE_STARTED string = "STAGE_REDUCE_STARTED" // reduce started
	STAGE_REDUCE_DONE    string = "STAGE_REDUCE_DONE"    // reduce has been done

	JOB_TYPE_MAP    string = "JOB_TYPE_MAP"
	JOB_TYPE_REDUCE string = "JOB_TYPE_REDUCE"
	JOB_TYPE_WAIT   string = "JOB_TYPE_WAIT"
	JOB_TYPE_DONE   string = "JOB_TYPE_DONE"
)

type Coordinator struct {
	// Your definitions here.
	ch            chan Job
	ch_completed  chan bool
	job_status    map[string]string
	mu            sync.Mutex
	stage_mu      sync.Mutex
	job_map_count int
	nReduce       int
	stage         string
}

type Job struct {
	JobId       int
	JobValue    string
	JobType     string
	NReduce     int
	ReduceIndex int
}

func (j *Job) getJobIdentify() string {
	return fmt.Sprintf("%s-%d", j.JobType, j.JobId)
}

type Bucket struct {
	KVs []KeyValue
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// retrieve a job from task pool
//
func (c *Coordinator) GetOneJob(args *BaseArgs, reply *JobReply) error {

	switch c.getStage() {
	case STAGE_INITIAL:
		reply.JobDetail.JobType = JOB_TYPE_WAIT
	case STAGE_MAP_DONE:
		reply.JobDetail.JobType = JOB_TYPE_WAIT
	case STAGE_MAP_STARTED:
		c.GetJob(args, reply)
	case STAGE_REDUCE_STARTED:
		c.GetJob(args, reply)
	case STAGE_REDUCE_DONE:
		reply.JobDetail.JobType = JOB_TYPE_DONE
	}

	return nil
}

func (c *Coordinator) getJobStatus(job_id string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.job_status[job_id]
}

func (c *Coordinator) updateJobStatus(job_id string, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.job_status[job_id] = status
}

func (c *Coordinator) GetJob(args *BaseArgs, reply *JobReply) error {
	// retrive a new job for task pool, with timeout = 100ms
	select {
	case res := <-c.ch:
		reply.JobDetail = res
		jobKey := reply.JobDetail.getJobIdentify()
		c.updateJobStatus(jobKey, JOB_STATUS_RETRIEVED)
		go func(job Job) {
			mJobKey := job.getJobIdentify()
			time.Sleep(10 * time.Second)
			if c.getJobStatus(mJobKey) != JOB_STATUS_COMPLETED {
				log.Printf("The job %s is overtime, it is pushed back to task pool.", mJobKey)
				c.ch <- reply.JobDetail
				c.updateJobStatus(mJobKey, JOB_STATUS_CREATED)
			} else {
				log.Printf("The submisison of job %s is accepted in time.", mJobKey)
				c.ch_completed <- true
			}
		}(reply.JobDetail)
	case <-time.After(100 * time.Millisecond):
		log.Printf("Timed out before get a new job\n")
		reply.JobDetail.JobType = JOB_TYPE_WAIT
	}
	return nil
}

//
// Notify the compeletion of a job
//
func (c *Coordinator) FinishOneJob(args *JobArgs, reply *BaseArgs) error {
	jobKey := args.JobDetail.getJobIdentify()

	if c.getJobStatus(jobKey) == JOB_STATUS_RETRIEVED {
		c.updateJobStatus(jobKey, JOB_STATUS_COMPLETED)
		log.Printf("The job %s is submitted successfully.", jobKey)
	} else {
		log.Printf("The job %s is over time. Refuse to accept the submission", jobKey)
	}

	return nil
}

func (c *Coordinator) getStage() string {
	c.stage_mu.Lock()
	defer c.stage_mu.Unlock()
	return c.stage
}

func (c *Coordinator) updateStage(next_stage string) {
	c.stage_mu.Lock()
	defer c.stage_mu.Unlock()
	c.stage = next_stage
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.getStage() == STAGE_REDUCE_DONE
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.job_map_count = len(files)
	c.nReduce = nReduce
	channel_len := c.job_map_count
	if channel_len < c.nReduce {
		channel_len = c.nReduce
	}
	c.ch_completed = make(chan bool, 1)
	c.ch = make(chan Job, channel_len)
	c.job_status = make(map[string]string, c.nReduce+c.job_map_count)
	c.updateStage(STAGE_INITIAL)
	for i, v := range files {
		go func(i int, v string) {
			job := Job{i, v, JOB_TYPE_MAP, c.nReduce, 0}
			c.ch <- job
			c.updateJobStatus(job.getJobIdentify(), JOB_STATUS_CREATED)
		}(i, v)
	}

	c.updateStage(STAGE_MAP_STARTED)

	go func() {
		// map jobs
		for i := 0; i < len(files); i++ {
			<-c.ch_completed
		}

		c.updateStage(STAGE_MAP_DONE)

		for i := 0; i < c.nReduce; i++ {
			job := Job{i, "", JOB_TYPE_REDUCE, c.nReduce, i}
			c.ch <- job
			c.updateJobStatus(job.getJobIdentify(), JOB_STATUS_CREATED)
		}

		c.updateStage(STAGE_REDUCE_STARTED)

		for i := 0; i < nReduce; i++ {
			<-c.ch_completed
		}
		c.updateStage(STAGE_REDUCE_DONE)
	}()

	c.server()
	return &c
}
