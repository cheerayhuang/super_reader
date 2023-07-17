package simhash

import (
    "bytes"
    "crypto/md5"
    "os"
    "regexp"
    "strings"
    //"fmt"

	"github.com/rs/zerolog"
)


var (
    enWordsRegexp = regexp.MustCompile(`\w+`)
    width = 3

    mLog = zerolog.New(os.Stdout).With().Str("module", "simhash").Timestamp().Logger()
)

func init() {
}


func slide(b []byte) [][]byte {
    res := make([][]byte, len(b)-width+1)
    for i := 0; i < len(res); i++ {
        res[i] = b[i:i+width]
    }

    return res
}

func addHashVals(sum []uint16, hash []byte) {
    for i, v := range hash {
        for j := 0; j < 8; j++ {
            sum[i*8+j] += uint16((v >> (7-j)) & 0x01)
        }
    }
}

func Tokenize(s *string) [][]byte {
    if (s == nil) {
        mLog.Warn().Msg("Dunno why s is a nil pointer.")
        return nil
    }
    ctn := strings.ToLower(*s)
    /*
    if (len(ctn) > 1e9) {
        mLog.Infof("len ctn: %d", len(ctn))

    }*/

    wordsArr := enWordsRegexp.FindAll([]byte(ctn), -1)
    /*
    if (len(wordsArr) > 1e9) {
        mLog.Infof("len ctn words: %d, one of them is %s.",
            len(wordsArr), string(wordsArr[len(wordsArr)-2]))
    }*/
    if len(wordsArr) < width {
        return nil
    }

    words := bytes.Join(wordsArr, []byte{})

    return slide(words)
}

func SimHashValue(s *string) (uint64, []uint16) {
    tokens := Tokenize(s)
    if tokens == nil {
        return 0, nil
    }
    tokenLen := len(tokens)

    sumFeatures := [64]uint16{0}
    for _, t := range tokens {
        hash := md5.Sum(t)
        lastMD58bytes := hash[8:]
        addHashVals(sumFeatures[0:], lastMD58bytes)

    }

    var res uint64 = 0
    var p uint64 = 1
    curBits := 0
    segments := make([]uint16, 4)

    for i := 63; i >= 0; i-- {
        if sumFeatures[i] > uint16(tokenLen / 2) {
            res += p
        }
        p *= 2
        curBits++
        if curBits % 16 == 0 {
            segments[4-curBits/16] = uint16(res >> (curBits-16))
        }
    }

    return res, segments
}

func TokenizeBytes(b []byte) [][]byte {
    if (b == nil) {
        mLog.Warn().Msg("Dunno why s is a nil pointer.")
        return nil
    }

    if len(b) < width {
        return nil
    }

    return slide(b)
}

func SimHashBytes(b []byte) (uint64, []uint16) {
    tokens := TokenizeBytes(b)
    if tokens == nil {
        return 0, nil
    }
    tokenLen := len(tokens)

    sumFeatures := [64]uint16{0}
    for _, t := range tokens {
        hash := md5.Sum(t)
        lastMD58bytes := hash[8:]
        addHashVals(sumFeatures[0:], lastMD58bytes)

    }

    var res uint64 = 0
    var p uint64 = 1
    curBits := 0
    segments := make([]uint16, 4)

    for i := 63; i >= 0; i-- {
        if sumFeatures[i] > uint16(tokenLen / 2) {
            res += p
        }
        p *= 2
        curBits++
        if curBits % 16 == 0 {
            segments[4-curBits/16] = uint16(res >> (curBits-16))
        }
    }

    return res, segments
}
