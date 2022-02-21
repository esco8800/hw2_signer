package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const COUT_ITER = 6

func main() {
	inputData := []int{0, 1}
	testResult := "NOT SET"

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
			fmt.Println("first job")
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			data, ok := dataRaw.(string)
			if !ok {
				panic("cant convert result data to string")
			}
			testResult = data
			fmt.Println("last job")
		}),
	}

	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)

	fmt.Println(testResult)
	fmt.Println(end)
}

func ExecutePipeline(jobs ...job)  {
	wg := &sync.WaitGroup{}
	in := make(chan interface{})

	for _, worker := range jobs {
		wg.Add(1)
		out := make(chan interface{})
		go executeWorker(worker, wg, in, out)
		in = out
	}
	wg.Wait()
}

func executeWorker(worker job, wg *sync.WaitGroup, in, out chan interface{})  {
	defer wg.Done()
	defer close(out)
	worker(in, out)
}

var SingleHash = func(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for val := range in {
		val := strconv.Itoa(val.(int))
		md5 := DataSignerMd5(val)
		wg.Add(1)
		go workerSingleHash(wg, val, md5, out)
	}

	wg.Wait()
	fmt.Println("End job SingleHash")
}

func workerSingleHash(wg *sync.WaitGroup, val string, md5 string, out chan interface{}) {
	defer wg.Done()

	ch32 := make(chan string)
	chMd5 := make(chan string)

	go calculateHash(ch32, val, DataSignerCrc32)
	go calculateHash(chMd5, md5, DataSignerCrc32)

	res32Hash := <-ch32
	resMd5Hash := <-chMd5

	res := res32Hash + "~" + resMd5Hash

	fmt.Println("SingleHash " + res)
	out <- res
}

func calculateHash(ch chan string, data string, f func(string) string){
	res := f(data)
	ch <- res
}

var MultiHash = func(in, out chan interface{}) {
	wg := &sync.WaitGroup{}

	for val := range in {
		wg.Add(1)
		go workerMultiHash(wg, val, out)
	}

	wg.Wait()
	fmt.Println("End job MultiHash")
}

func workerMultiHash(wg *sync.WaitGroup, val interface{}, out chan interface{}) {
	defer wg.Done()
	hashes := make(chan string, COUT_ITER)

	wgMulti := &sync.WaitGroup{}
	for i := 0; i < COUT_ITER; i++ {
		wgMulti.Add(1)
		go calculateMultiHash(wgMulti, hashes, val.(string), i)
	}
	wgMulti.Wait()

	res := ""
	for i := 0; i < COUT_ITER; i++ {
		res += <- hashes
	}

	fmt.Println("MultiHash " + res)
	out <- res
}

func calculateMultiHash(wg *sync.WaitGroup, hashes chan string, val string, i int) {
	defer wg.Done()
	hashes <- DataSignerCrc32(strconv.Itoa(i) + val)
}

var CombineResults = func(in, out chan interface{}) {
	res := make([]string, 0)
	for val := range in {
		res = append(res, val.(string))
	}
	// Сортировка слайса
	sort.Slice(res, func(i, j int) bool {
		return res[i] < res[j]
	})
	strRes := strings.Join(res, "_")

	out <- strRes

	fmt.Println("CombineResults " + strRes)
	fmt.Println("End job CombineResults")
}