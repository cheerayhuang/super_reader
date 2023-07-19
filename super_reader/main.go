package main

import (
    "flag"
	"os"
    "time"

	"super_reader/super_io"

	"github.com/rs/zerolog"
)
var (
    mLog zerolog.Logger

    filePath *string
    goroutineNum *int
    readingChunkSize *uint64
    outputDir *string
    dupNum *int
)

func init() {
    zerolog.TimeFieldFormat = time.RFC3339
    mLog = zerolog.New(os.Stdout).With().Str("module", "main").Timestamp().Logger()

    debug := flag.Bool("debug", false, "Run in debug mode.")
    filePath = flag.String("path", "./demo.txt", "The FILE to read.")
    goroutineNum = flag.Int("c", 1, "Concurrency, the total of goroutines.")
    readingChunkSize = flag.Uint64("chunk", 4096, "Bytes need reading per Read() calling. 2M - 2097152, 4M - 4194304, 8M - 8388608, 16M - 16777216, 32M - 33554432.")

    outputDir = flag.String("out", "./out", "Set output directiory.")
    dupNum = flag.Int("dupnum", 1, "Set to 2, when dedup itself.")
    flag.Parse()

    if *debug {
        zerolog.SetGlobalLevel(zerolog.DebugLevel)
    } else {
        zerolog.SetGlobalLevel(zerolog.WarnLevel)
    }
}



func main() {
    mLog.Debug().Msg("hello super reader.")

    reader := super_io.NewFileReader(filePath, *goroutineNum, *readingChunkSize)
    mLog.Debug().Msgf("reader: %+v", reader)

    //reader.ReadAsJsonL()
    reader.ReadAsJsonL().
        LogResult().LogTimer().DedupResultsWithSimHash(*dupNum)

    reader.WriteDedupResult(outputDir)
}
