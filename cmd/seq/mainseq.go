package main

import (
	"fmt"
	"log"
	"mapreduce-tp/seq"  
	"os"
	"plugin"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: go run mainseq.go plugin.so file1.txt file2.txt ...\n")
		os.Exit(1)
	}

	pluginFile := os.Args[1]
	files := os.Args[2:]

	// Cargar el plugin (.so)
	p, err := plugin.Open(pluginFile)
	if err != nil {
		log.Fatalf("cannot load plugin %v: %v", pluginFile, err)
	}

	// Buscar función Map
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v: %v", pluginFile, err)
	}
	mapf, ok := xmapf.(func(string, string) []mapreduceseq.KeyValue)
	if !ok {
		log.Fatalf("Map has wrong signature in %v", pluginFile)
	}

	// Buscar función Reduce
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v: %v", pluginFile, err)
	}
	reducef, ok := xreducef.(func(string, []string) string)
	if !ok {
		log.Fatalf("Reduce has wrong signature in %v", pluginFile)
	}

	// Ejecutar secuencial
	result := mapreduceseq.Sequential(files, mapf, reducef)

	// Mostrar resultados
	for _, kv := range result {
		fmt.Printf("%v %v\n", kv.Key, kv.Value)
	}
}
