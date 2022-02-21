package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
		//job(MultiHash),
		//job(CombineResults),
		//job(func(in, out chan interface{}) {
		//	dataRaw := <-in
		//	data, ok := dataRaw.(string)
		//	if !ok {
		//		panic("cant convert result data to string")
		//	}
		//	testResult = data
		//	fmt.Println("last job")
		//}),
	}

	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)

	fmt.Println(testResult)
	fmt.Println(end)
}

func ExecutePipeline(jobs ...job)  {
	wg := &sync.WaitGroup{}

	firstJob := jobs[0]
	in := gen(firstJob, wg)
	jobs = append(jobs[:0], jobs[1:]...)

	SingleHash := func () chan interface{} {
		out := make(chan interface{})
		wg.Add(1)
		go func (wg *sync.WaitGroup) {
			defer wg.Done()
			SingleHash(in, out)
		}(wg)
		return out
	}

	MultiHash := func () chan interface{} {
		out := make(chan interface{})
		wg.Add(1)
		go func (wg *sync.WaitGroup) {
			defer wg.Done()
			MultiHash(SingleHash(), out)
		}(wg)
		return out
	}

	CombineResults := func () chan interface{} {
		out := make(chan interface{})
		wg.Add(1)
		go func (wg *sync.WaitGroup) {
			defer wg.Done()
			CombineResults(MultiHash(), out)
		}(wg)
		return out
	}

	CombineResults()
	wg.Wait()
}

func gen(firstJob job,wg *sync.WaitGroup) chan interface{} {
	out := make(chan interface{})
	wg.Add(1)
	go func (wg *sync.WaitGroup) {
		defer wg.Done()
		firstJob(make(chan interface{}), out)
		close(out)
	}(wg)
	return out
}

var SingleHash = func(in, out chan interface{}) {
	for val := range in {
		res := DataSignerCrc32(strconv.Itoa(val.(int))) + "~" + DataSignerCrc32(DataSignerMd5(strconv.Itoa(val.(int))))
		fmt.Println("SingleHash " + res)
		out <- res
	}
	close(out)
	fmt.Println("End job SingleHash")
}

var MultiHash = func(in, out chan interface{}) {
	for val := range in {
		res := ""
		for i := 0; i < 6; i++ {
			res += DataSignerCrc32(strconv.Itoa(i) + val.(string))
		}
		fmt.Println("MultiHash " + res)
		out <- res
	}
	close(out)
	fmt.Println("End job MultiHash")
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

	//out <- strRes
	close(out)

	fmt.Println("CombineResults " + strRes)
	fmt.Println("End job CombineResults")
}