package super_io

import (
	"bufio"
    "bytes"
	"time"
	/*
		"encoding/json"
	*/
	"errors"
	"io"
	"os"
	//"strings"
	"sync"
	/*
		"sync/atomic"
		"time"
	*///log "github.com/sirupsen/logrus"
)


type FileReader struct {
    FileName string
    FHandlers []*os.File

    readingSlices [] uint64
    lenSlices int
    readingChunkSize uint64

    results [][]byte

    wgRead sync.WaitGroup

    resChan chan []byte

    perfElapsed time.Duration
}

func NewFileReader(path *string, goroutineNum int, readingChunkSize uint64) *FileReader {
    fileInfo, err := os.Stat(*path)
    if err != nil {
        mLog.Error().Err(err).Send()
        return nil
    }

    if fileInfo.IsDir(){
        mLog.Error().Err(errors.New("can't handle directory.")).Send()
        return nil
    }

    fileSize := fileInfo.Size()
    mLog.Info().Str("path", *path).Int64("fileSize", fileSize).Send()

    sliceSize := fileSize / int64(goroutineNum)
    mLog.Info().Int64("sliceSize", sliceSize).Msgf("Separate this file to %d parts.", goroutineNum)

    f := new(FileReader)

    f.FileName = *path

    f.FHandlers = make([]*os.File, goroutineNum)
    f.readingSlices = make([]uint64, goroutineNum+1)
    f.lenSlices = goroutineNum
    f.readingChunkSize = readingChunkSize
    if readingChunkSize >= uint64(sliceSize) {
        f.readingChunkSize /= 3
    }
    f.results = make([][]byte, 0)
    f.resChan = make(chan []byte, f.lenSlices*8)

    var seekIndex int64 = 0
    for i := 0; i < goroutineNum; i++ {
        h, _ := os.Open(*path)
        h.Seek(seekIndex, io.SeekStart)
        f.FHandlers[i] = h
        f.readingSlices[i] = uint64(seekIndex)
        seekIndex += sliceSize
    }

    f.readingSlices[goroutineNum] = uint64(fileSize)

    return f
}


func (f *FileReader) Read(p []byte) (n int, err error) {
    return 0, nil
}

func (f *FileReader) ReadAsJsonL() (*FileReader) {
    perfStartTime := time.Now()

    go f.asyncResults()
    for i := 0; i < f.lenSlices; i++ {
        f.wgRead.Add(1)
        go f.reading(i)
    }

    f.wgRead.Wait()
    close(f.resChan)

    f.perfElapsed = time.Since(perfStartTime)

    return f
}

func (f *FileReader) LogTimer() (*FileReader) {
    elapsedMicro := f.perfElapsed.Microseconds()
    mLog.Log().
        Int64("Reading Elapsed(micro)", elapsedMicro).
        Float32("Reading Elapsed(ms)", float32(elapsedMicro) / 1e3).
        Float32("Reading Elapsed(s)", float32(elapsedMicro) / 1e6).
        Send()

    return f
}

func (f *FileReader) LogResult() (*FileReader) {
    for i, r := range f.results {
            mLog.Log().Int("lineNum", i+1).RawJSON("line", r).Send()
    }

    return f
}

func (f *FileReader) asyncResults() {
    for r := range f.resChan {
        f.results = append(f.results, r)
    }
}

func (f* FileReader) reading(goIndex int) {

    defer f.wgRead.Done()

    // shouldn't be executed.
    if err := f.correctReadingEndIndex(goIndex); err != nil {
        return
    }

    alreadyReadBytes := uint64(0)
    b := bufio.NewReader(f.FHandlers[goIndex])

    bufpool := sync.Pool{New: func() interface{} {
	    block := make([]byte, f.readingChunkSize + f.readingChunkSize/2)
		return block
	}}


    for alreadyReadBytes + f.readingSlices[goIndex] <= f.readingSlices[goIndex+1] {
        buf := bufpool.Get().([]byte)
        actualBufBytes := f.readingChunkSize
        if f.readingChunkSize + alreadyReadBytes + f.readingSlices[goIndex] > f.readingSlices[goIndex+1] {
            actualBufBytes = f.readingSlices[goIndex+1] - f.readingSlices[goIndex] - alreadyReadBytes
        }
        mLog.Debug().Msgf("actualBufBytes: %d, alreadyReadBytes: %d, needReadingBytes: %d, %d", actualBufBytes, alreadyReadBytes, f.readingSlices[goIndex+1], f.readingSlices[goIndex])

        n, err := io.ReadFull(b, buf[0:actualBufBytes])
        mLog.Debug().Uint64("actualBufBytes", actualBufBytes).Err(err).Msgf("Read %d bytes.", n)
        if err != nil {
            if err == io.EOF {
                mLog.Info().Msgf("read file <%s> finished.", f.FileName)
                if n == 0 {
                    break
                }
            } else {
                mLog.Fatal().Caller().Err(err).Send()
            }
        }
        alreadyReadBytes += uint64(n)

        /*
        if uint64(n) != actualBufBytes {
            mLog.Panic().Int("readN", n).Uint64("actualBufBytes", actualBufBytes).Msg("io.ReadFull can't read fully.")
        }*/
        bytesRead := buf[0:n]

        bytesUntilNewLine, err := b.ReadBytes('\n')
        if err != nil {
            if err == io.EOF {
                mLog.Info().Msgf("read file <%s> finished while reading an extra line.", f.FileName)
            } else {
                mLog.Fatal().Caller().Err(err).Send()
            }
        }
        bytesRead = append(bytesRead, bytesUntilNewLine...)
        alreadyReadBytes += uint64(len(bytesUntilNewLine))

        linesBytes := bytes.Split(bytesRead, []byte{'\n'})
        mLog.Debug().Msgf("len(buf): %d, len(bytesRead): %d, len(linesBytes): %d", len(buf), len(bytesRead), len(linesBytes))

        for _, l := range linesBytes {
            if len(l) == 0 {
                continue
            }

            resline := make([]byte, len(l))
            copy(resline, l)
            f.resChan <- resline

            mLog.Debug().RawJSON("line", l).Send()
        }

        mLog.Debug().
            Msgf("buf addr: %x, bytesRead addr: %x, linesBytes addr %x", &buf[0], &bytesRead[0], &linesBytes[0][0])
        if len(bytesRead) > len(buf) {
            bufpool.Put(bytesRead)
        } else {
            bufpool.Put(buf)
        }
    }
}

func (f* FileReader) correctReadingEndIndex(goIndex int) error {
    if (goIndex == 0) {
        return nil
    }

    h := f.FHandlers[goIndex]
    b := bufio.NewReader(h)

    bytesUntilNewLine, err := b.ReadBytes('\n')
    if err != nil {
        mLog.Fatal().Caller().Err(err).Send()
        return err
    }

    f.readingSlices[goIndex] += uint64(len(bytesUntilNewLine))
    f.FHandlers[goIndex] = h

    return nil
}
