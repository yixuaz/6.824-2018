package mapreduce

import (
	"encoding/json"
	"log"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//step 1 open every intermediate file to read key value into memory
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		file,err := os.Open(reduceName(jobName,i,reduceTask))
		if err != nil {
			log.Fatal("create intermediate file failed", reduceName(jobName,i,reduceTask))
		}
		var kv KeyValue
		dec := json.NewDecoder(file)
		err = dec.Decode(&kv)
		for err == nil {
			keyValues[kv.Key] = append(keyValues[kv.Key],kv.Value)
			err = dec.Decode(&kv)
		}
		file.Close()
	}
	//step 2 sort key
	var keys []string
	for k := range keyValues {
		keys = append(keys,k)
	}
	sort.Strings(keys)
	//step 3 foreach key call reduceF then write to output
	out, err := os.Create(outFile)
	if err != nil {
		log.Fatal("failed to create outfile",outFile)
	}
	enc := json.NewEncoder(out)
	for _,k := range keys {
		v := reduceF(k,keyValues[k])
		err = enc.Encode(KeyValue{k,v})
		if err != nil {
			log.Fatal("failed to encode",KeyValue{k,v})
		}
	}
	out.Close()
}

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
