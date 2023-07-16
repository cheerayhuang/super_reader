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
)

func init() {
    zerolog.TimeFieldFormat = time.RFC3339
    mLog = zerolog.New(os.Stdout).With().Str("module", "main").Timestamp().Logger()

    debug := flag.Bool("debug", false, "Run in debug mode.")
    filePath = flag.String("path", "./demo.txt", "The FILE to read.")
    goroutineNum = flag.Int("c", 1, "Concurrency, the total of goroutines.")
    readingChunkSize = flag.Uint64("chunk", 4096, "Bytes need reading per Read() calling.")
    flag.Parse()

    if *debug {
        zerolog.SetGlobalLevel(zerolog.DebugLevel)
    } else {
        zerolog.SetGlobalLevel(zerolog.WarnLevel)
    }
}



func main() {
    //mLog.Fatal().Msg("test fatal")
    //mLog.Panic().Msg("test panic")
    mLog.Debug().Msg("hello super reader.")

    reader := super_io.NewFileReader(filePath, *goroutineNum, *readingChunkSize)
    mLog.Debug().Msgf("reader: %+v", reader)

    reader.ReadAsJsonL()//.LogResult().LogTimer()
}
