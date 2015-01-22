package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	I "github.com/brentp/irelate"
)

func init() {
	// so that output stops when piping to head.
	done := make(chan os.Signal, 1)

	signal.Notify(done, os.Interrupt, syscall.SIGIO, syscall.SIGPIPE)
	go func() {
		for _ = range done {
			os.Exit(0)
		}
	}()
}

func main() {

	cpuProfile := flag.Bool("cpuProfile", false, "perform CPU profiling")
	flag.Parse()
	files := flag.Args()

	streams := make([]I.RelatableChannel, 0)
	for _, f := range files {
		// Streamer automatically returns a Relatalbe Channel for bam/gff/bed(.gz)
		streams = append(streams, I.Streamer(f))
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

	merged := I.Merge(streams...)
	for interval := range I.IRelate(merged, I.CheckRelatedByOverlap, false, 0) {
		// for bam output:
		bam := (interval).(*I.Bam)
		fmt.Fprintf(buf, "%s\t%d\t%d\t%s\t%v\n", bam.Chrom(), bam.Start(), bam.End(), bam.Cigar(), bam.Name(), bam.Seq())

	}
	buf.Flush()

}
