package main

import (
	mapreduceseq "mapreduce-tp/mapreduce"
	"sort"
	"strings"
	"unicode"
)

// Map: produce pares (word, docID)
func Map(filename string, contents string) []mapreduceseq.KeyValue {
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})

	kva := []mapreduceseq.KeyValue{}
	for _, w := range words {
		word := strings.ToLower(w)
		kva = append(kva, mapreduceseq.KeyValue{Key: word, Value: filename})
	}
	return kva
}

// Reduce: recibe (word, [docIDs...]) y devuelve (word, lista ordenada y única de docIDs)
func Reduce(key string, values []string) string {
	seen := make(map[string]bool)
	unique := []string{}

	for _, doc := range values {
		if !seen[doc] {
			seen[doc] = true
			unique = append(unique, doc)
		}
	}

	sort.Strings(unique)

	return strings.Join(unique, ",")
}
