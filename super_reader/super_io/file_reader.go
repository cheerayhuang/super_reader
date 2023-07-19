package super_io

import (
	"bufio"
	"bytes"
	/*
		"encoding/json"
	*/
	"errors"
	"io"
	"os"

	//"strings"
	"regexp"
	"strconv"
	"sync"
	"time"

	//"sync/atomic"
	//log "github.com/sirupsen/logrus"

	"super_reader/simhash"
)

type Result struct {
    SimHash uint64
    SegKeys []uint16

    ResBytes []byte
}

type FileReader struct {
    FileName string
    FHandlers []*os.File

    readingSlices [] uint64
    lenSlices int
    readingChunkSize uint64

    //results [][]byte
    results []*Result

    wg sync.WaitGroup

    resChan chan *Result
    resAsyncStop chan bool

    perfElapsed time.Duration

    resIndices *simhash.SimHashIndex
    resDedupBySimHash []*Result

    countSlicesClosed int32
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
        f.readingChunkSize = uint64(sliceSize / 3)
    }
    f.results = make([]*Result, 0)
    f.resChan = make(chan *Result, f.lenSlices*10)
    f.resAsyncStop = make(chan bool, 1)

    var seekIndex int64 = 0
    for i := 0; i < goroutineNum; i++ {
        h, _ := os.Open(*path)
        h.Seek(seekIndex, io.SeekStart)
        f.FHandlers[i] = h
        f.readingSlices[i] = uint64(seekIndex)
        seekIndex += sliceSize
    }
    f.readingSlices[goroutineNum] = uint64(fileSize)

    f.resIndices = simhash.NewSimHashIndex()
    f.resDedupBySimHash = make([]*Result, 0)

    return f
}


func (f *FileReader) Read(p []byte) (n int, err error) {
    return 0, nil
}

func (f *FileReader) constructIndices(r []byte) *Result {
    s, seg := simhash.SimHashBytes(r)
    mLog.Debug().Msgf("simhash: %x, seg: %v", s, seg)

    m := new(simhash.LineMeta)
    m.SimHash = s

    f.resIndices.Insert(m, seg)

    res := new(Result)
    res.SimHash = s
    res.SegKeys = seg
    res.ResBytes = r

    //f.results = append(f.results, res)
    return res
}

func (f *FileReader) DedupResultsWithSimHash(dupNum int) (*FileReader) {

    lenSubRes := len(f.results) / f.lenSlices
    ch := make(chan *Result, f.lenSlices*10)
    stopCh := make(chan bool, 1)
    for i := 0; i < f.lenSlices; i++ {
        f.wg.Add(1)
        if (i == f.lenSlices-1) {
            go f.dedupSubRes(f.results[i*lenSubRes:], dupNum, ch, stopCh)
        } else {
            go f.dedupSubRes(f.results[i*lenSubRes:(i+1)*lenSubRes], dupNum, ch, stopCh)
        }
    }

    dedupSubResDone := 0
    for {
        needClosing := false
        select {
        case r := <- ch:
            f.resDedupBySimHash = append(f.resDedupBySimHash, r)

        case <-stopCh:
            dedupSubResDone++
            if dedupSubResDone == f.lenSlices {
                needClosing = true
                break
            }

        default:
        }

        if needClosing {
            close(ch)
            close(stopCh)
            break
        }
    }
    f.wg.Wait()

    mLog.Log().Int("Total dedup results", len(f.resDedupBySimHash)).Send()
    return f
}

func (f *FileReader) dedupSubRes(subResults []*Result, dupNum int, ch chan *Result, stopCh chan bool) {
    defer f.wg.Done()

    for _, r := range subResults {
        m := new(simhash.LineMeta)
        m.SimHash = r.SimHash

        if res, _ := f.resIndices.NearBy(m, r.SegKeys, dupNum);  res != nil {
            mLog.Info().Msgf("[%x] is similar with [%x]", m.SimHash, res[0].SimHash)
            continue
        }

        ch <- r
    }

    stopCh <- true
}

func (f *FileReader) ReadAsJsonL() (*FileReader) {
    perfStartTime := time.Now()

    go f.asyncResults()
    for i := 0; i < f.lenSlices; i++ {
        f.wg.Add(1)
        go f.correctReadingEndIndex(i)
    }
    f.wg.Wait()

    for i := 0; i < f.lenSlices; i++ {
        f.wg.Add(1)
        go f.reading(i)
    }
    f.wg.Wait()

    for {
        needClosing := false
        select {
        case <-f.resAsyncStop:
            needClosing = true
            break
        default:
        }

        if needClosing {
            close(f.resChan)
            break
        }
    }
    f.perfElapsed = time.Since(perfStartTime)

    return f
}

func (f *FileReader) WriteDedupResult(outputDir *string) {
    if err := os.MkdirAll(*outputDir + "/" + f.FileName + "/", 0750); err != nil {
        mLog.Fatal().Err(err).Msg("Create output dir failed.")
    }

    subLen := len(f.resDedupBySimHash) / f.lenSlices

    for i := 0; i < f.lenSlices; i++ {
        f.wg.Add(1)
        path := *outputDir + "/" + f.FileName + "/" + strconv.Itoa(i) + ".jsonl"
        if i == f.lenSlices - 1 {
            go f.writeResLine(f.resDedupBySimHash[i*subLen:], path)
        } else {
            go f.writeResLine(f.resDedupBySimHash[i*subLen:(i+1)*subLen], path)
        }
    }
    f.wg.Wait()
}

func (f *FileReader) writeResLine(subRes []*Result, path string) {
    defer f.wg.Done()

    outFile, err := os.Create(path)
    if err != nil {
        mLog.Fatal().Err(err).Msg("Create output file failed.")
    }
    defer outFile.Close()

    w := bufio.NewWriter(outFile)
    for _, r := range subRes  {
        w.WriteString(string(r.ResBytes)+"\n")
    }

    w.Flush()
    mLog.Log().Msgf("output file <%s> DONE.", path)
}

func (f *FileReader) LogTimer() (*FileReader) {
    elapsedMicro := f.perfElapsed.Microseconds()
    mLog.Log().
        Int64("Read Elapsed(micro)", elapsedMicro).
        Float32("Read Elapsed(ms)", float32(elapsedMicro) / 1e3).
        Float32("Read Elapsed(s)", float32(elapsedMicro) / 1e6).
        Send()

    return f
}

func (f *FileReader) LogResult() (*FileReader) {
    for i, r := range f.results {
        mLog.Info().Int("lineNum", i+1).Str("line", string(r.ResBytes[0:len(r.ResBytes)/10])+string(r.ResBytes[len(r.ResBytes)/10*9:])).Send()
        if r.ResBytes[len(r.ResBytes)-1] != '}' {
            mLog.Error().Int("lineNum", i+1).Msg("read data lines ERROR!")
        }
    }
    mLog.Log().Int("Total results", len(f.results)).Send()
    //mLog.Log().Int("Total dedup results", len(f.resDedupBySimHash)).Send()

    return f
}

func (f *FileReader) asyncResults() {
    countSlicesClosed := 0

    re := regexp.MustCompile(`^(\d+)#END$`)
    for r := range f.resChan {
        if matchRes := re.FindSubmatch(r.ResBytes); matchRes != nil {
            goIndex, _ := strconv.Atoi(string(matchRes[1]))
            f.FHandlers[goIndex].Close()
            f.FHandlers[goIndex] = nil

            countSlicesClosed++
            if (countSlicesClosed == f.lenSlices) {
                close(f.resAsyncStop)
                break
            }
            continue
        }
        f.results = append(f.results, r)
    }
}

func (f* FileReader) reading(goIndex int) {

    defer f.wg.Done()

    alreadyReadBytes := uint64(0)
    b := bufio.NewReader(f.FHandlers[goIndex])

    bufpool := sync.Pool{New: func() interface{} {
	    block := make([]byte, f.readingChunkSize + f.readingChunkSize/2)
		return block
	}}

    for alreadyReadBytes + f.readingSlices[goIndex] < f.readingSlices[goIndex+1] {
        buf := bufpool.Get().([]byte)
        actualBufBytes := f.readingChunkSize
        if f.readingChunkSize + alreadyReadBytes + f.readingSlices[goIndex] > f.readingSlices[goIndex+1] {
            actualBufBytes = f.readingSlices[goIndex+1] - f.readingSlices[goIndex] - alreadyReadBytes
        }
        mLog.Debug().Int("go", goIndex).Msgf("actualBufBytes: %d, alreadyReadBytes: %d, needReadingBytes: %d, %d",
            actualBufBytes, alreadyReadBytes, f.readingSlices[goIndex], f.readingSlices[goIndex+1])

        n, err := io.ReadFull(b, buf[0:actualBufBytes])
        mLog.Debug().Uint64("actualBufBytes", actualBufBytes).Int("go", goIndex).Err(err).Msgf("Read %d bytes.", n)
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

        if bytesRead[n-1] != '\n' {
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
        }

        linesBytes := bytes.Split(bytesRead, []byte{'\n'})
        mLog.Debug().Int("go", goIndex).Msgf("len(buf): %d, len(bytesRead): %d, len(linesBytes): %d", len(buf), len(bytesRead), len(linesBytes))

        for _, l := range linesBytes {
            if len(l) == 0 {
                continue
            }

            resline := make([]byte, len(l))
            copy(resline, l)
            r := f.constructIndices(resline)
            f.resChan <- r

            //mLog.Debug().Int("go", goIndex).RawJSON("line", l).Send()
            mLog.Debug().Int("go", goIndex).Msgf("line: %s", string(l[0:len(l)/10]))
        }

        if len(linesBytes[0]) != 0 && len(bytesRead) != 0 {
            mLog.Debug().
                Msgf("buf addr: %x, bytesRead addr: %x, linesBytes addr %x", &buf[0], &bytesRead[0], &linesBytes[0][0])
        }
        if len(bytesRead) > len(buf) {
            bufpool.Put(bytesRead)
        } else {
            bufpool.Put(buf)
        }
    }
    endR := new(Result)
    endR.ResBytes = []byte(strconv.Itoa(goIndex)+"#END")
    f.resChan <- endR
    mLog.Log().Msgf("go: %d END.", goIndex)
}

func (f* FileReader) correctReadingEndIndex(goIndex int) error {
    defer f.wg.Done()

    if (goIndex == 0) {
        return nil
    }

    h := f.FHandlers[goIndex]
    b := bufio.NewReader(h)

    bytesUntilNewLine, err := b.ReadBytes('\n') // this API will read more bytes, and then find '\n' in them.
    if err != nil {
        mLog.Fatal().Caller().Err(err).Send()
    }

    f.readingSlices[goIndex] += uint64(len(bytesUntilNewLine))

    mLog.Debug().Msgf("modify %d readingSlices, new value: %d, skip text: %s",
        goIndex, f.readingSlices[goIndex],
        string(bytesUntilNewLine[len(bytesUntilNewLine)-5:]))

    // should use absoulute position to seek.
    h.Seek(int64(f.readingSlices[goIndex]), io.SeekStart)
    //h.Seek(int64(len(bytesUntilNewLine)), io.SeekCurrent)
    //f.FHandlers[goIndex] = h

    return nil
}
