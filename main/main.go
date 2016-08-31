package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/brentp/bix"
	"github.com/brentp/irelate"
	I "github.com/brentp/irelate/interfaces"
)

func init() {
	// so that output stops when piping to head.
	done := make(chan os.Signal, 1)

	signal.Notify(done, os.Interrupt, syscall.SIGIO, syscall.SIGPIPE)
	go func() {
		//for range done {
		// for travis
		for _ = range done {
			os.Exit(0)
		}
	}()
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func main() {
	f, err := os.Create("irelate.cpu.pprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	files := os.Args[1:]
	buf := bufio.NewWriter(os.Stdout)
	b, err := bix.New(files[0], 1)
	check(err)
	bx, err := b.Query(nil)
	check(err)

	queryables := make([]I.Queryable, len(files)-1)
	for i, f := range files[1:] {
		q, err := bix.New(f, 1)
		if err != nil {
			log.Fatal(err)
		}
		queryables[i] = q
	}

	for interval := range irelate.PIRelate(4000, 25000, bx, false, nil, queryables...) {
		fmt.Fprintf(buf, "%s\t%d\t%d\t%d\n", interval.Chrom(), interval.Start(), interval.End(), len(interval.Related()))
	}
	buf.Flush()
}
