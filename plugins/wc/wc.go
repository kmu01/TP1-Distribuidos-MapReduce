package main

import (
	mapreduceseq "mapreduce-tp/seq"
	"strconv"
	"strings"
	"unicode"
)

// Map: produce pares (word, "1")
func Map(filename string, contents string) []mapreduceseq.KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	kva := []mapreduceseq.KeyValue{}
	for _, w := range words {
		kva = append(kva, mapreduceseq.KeyValue{Key: strings.ToLower(w), Value: "1"})
	}
	return kva
}

// Reduce: recibe (word, ["1", "1", ...]) y devuelve (word, count)
func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}