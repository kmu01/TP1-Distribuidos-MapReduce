package mapreduceseq

import (
	"log"
	"os"
	"sort"
)

type KeyValue struct {
    Key   string
    Value string
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string

func Sequential(files []string, mapf MapFunc, reducef ReduceFunc) []KeyValue {
	var intermediate []KeyValue

	// Map
	for _, filename := range files {
		content, err := os.ReadFile(filename)
		if err != nil {
			log.Fatalf("cannot read %v: %v", filename, err)
		}
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	} 

	// Agrupar por key
	groups := make(map[string][]string)
	for _, kv := range intermediate {
		groups[kv.Key] = append(groups[kv.Key], kv.Value)
	}

	// Reduce
	var results []KeyValue
	for k, v := range groups {
		output := reducef(k, v)
		results = append(results, KeyValue{Key: k, Value: output})
	}

	// Ordenar para salida determinista
	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results
}
