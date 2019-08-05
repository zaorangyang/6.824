package mapreduce

import (
	"encoding/json"
	"os"
	"sort"
)

func readIntermediateFile(jobName string, reduceTask int, nMap int) []KeyValue {
	readed := []KeyValue{}
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)
		file, err := os.Open(fileName)
		if err != nil {
			debug("doReduce err %v", err)
			return nil
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			readed = append(readed, kv)
		}
	}
	return readed
}

type reduceParam struct {
	key    string
	values []string
}

func getRecudeParams(readed []KeyValue) []reduceParam {
	if len(readed) == 0 {
		return nil
	}
	params := []reduceParam{}
	preKey := readed[0].Key
	values := []string{readed[0].Value}
	for i := 1; i < len(readed); i++ {
		data := readed[i]
		if data.Key == preKey {
			values = append(values, data.Value)
		} else {
			params = append(params, reduceParam{
				key:    preKey,
				values: values,
			})
			values = []string{readed[i].Value}
			preKey = data.Key
		}
	}
	params = append(params, reduceParam{
		key:    preKey,
		values: values,
	})
	return params
}

func getReduceRets(params []reduceParam, reduceF func(key string, values []string) string) []KeyValue {
	rets := []KeyValue{}
	for _, param := range params {
		ret := reduceF(param.key, param.values)
		rets = append(rets, KeyValue{
			Key:   param.key,
			Value: ret,
		})
	}
	return rets
}

func writeOutFile(reduceRets []KeyValue, outFile string) {
	file, _ := os.Create(outFile)
	defer file.Close()
	enc := json.NewEncoder(file)
	for _, ret := range reduceRets {
		enc.Encode(&ret)
	}
}

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//

	readed := readIntermediateFile(jobName, reduceTask, nMap)
	if len(readed) == 0 {
		return
	}
	sort.Slice(readed, func(i, j int) bool {
		return readed[i].Key < readed[j].Key
	})
	params := getRecudeParams(readed)
	reduceRets := getReduceRets(params, reduceF)
	writeOutFile(reduceRets, outFile)
}
