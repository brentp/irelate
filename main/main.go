package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

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
func main0() {
	files := os.Args[1:]
	buf := bufio.NewWriter(os.Stdout)
	log.Println(files)
	for interval := range irelate.PIRelate(1000, 10, "", files[0], files[1:]...) {
		fmt.Fprintf(buf, "%s\t%d\t%d\t%d\n", interval.Chrom(), interval.Start(), interval.End(), len(interval.Related()))
	}
	buf.Flush()
}

func main() {

	cpuProfile := flag.Bool("cpuProfile", false, "perform CPU profiling")
	flag.Parse()
	files := flag.Args()

	streams := make([]I.RelatableChannel, 0)
	for _, f := range files {
		// Streamer automatically returns a Relatalbe Channel for bam/gff/bed(.gz)
		log.Println(f)
		s, _ := irelate.Streamer(f, "")
		streams = append(streams, s)
	}

	if *cpuProfile {
		f, err := os.Create("irelate.cpu.pprof")
		if err != nil {
			panic(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	buf := bufio.NewWriter(os.Stdout)

	//for interval := range I.IRelate(merged, I.CheckRelatedByOverlap) {
	for interval := range irelate.IRelate(irelate.CheckRelatedByOverlap, 0, irelate.Less, streams...) {
		// for bam output:
		// bam := *(interval).(*I.Bam)
		fmt.Fprintf(buf, "%s\t%d\t%d\t%d\n", interval.Chrom(), interval.Start(), interval.End(), len(interval.Related()))

	}
	buf.Flush()
}
