package simhash

import (
    "sync"
)

var (
    indexRwLock sync.RWMutex
)

const (
    dupThreshold = 4
)

type LineMeta struct {
    SimHash uint64
    //LineNum int
    FileName   string
}

type LineMetaList = []*LineMeta

type indexSubMap = map[uint16]LineMetaList
type indexSubMap32 = map[uint32]LineMetaList

type SimHashIndex struct {
    indexHi32Hi16 indexSubMap
    indexHi32Lo16 indexSubMap
    indexLo32Hi16 indexSubMap
    indexLo32Lo16 indexSubMap

    indexHi32 indexSubMap32
    indexLo32 indexSubMap32
}

func NewSimHashIndex() *SimHashIndex {
    r := new(SimHashIndex)

    r.indexHi32Hi16 = make(indexSubMap)
    r.indexHi32Lo16 = make(indexSubMap)
    r.indexLo32Hi16 = make(indexSubMap)
    r.indexLo32Lo16 = make(indexSubMap)

    r.indexHi32 = make(indexSubMap32)
    r.indexLo32 = make(indexSubMap32)

    return r
}

func (s *SimHashIndex) Insert(m *LineMeta, keys []uint16) error {
    /*
    indexRwLock.Lock()
    defer indexRwLock.Unlock()
    */

    /*
    s.indexHi32Hi16[keys[0]] = append(s.indexHi32Hi16[keys[0]], m)
    s.indexHi32Lo16[keys[1]] = append(s.indexHi32Lo16[keys[1]], m)
    s.indexLo32Hi16[keys[2]] = append(s.indexLo32Hi16[keys[2]], m)
    s.indexLo32Lo16[keys[3]] = append(s.indexLo32Lo16[keys[3]], m)
    */

    keyHi32 := (uint32(keys[0]) << 16) + uint32(keys[1])
    keyLo32 := (uint32(keys[2]) << 16) + uint32(keys[3])

    s.indexHi32[keyHi32] = append(s.indexHi32[keyHi32], m)
    s.indexLo32[keyLo32] = append(s.indexHi32[keyLo32], m)

    return nil
}

func (s* SimHashIndex) NearBy(m *LineMeta, keys []uint16) (LineMetaList, error) {

    //resMap := make(map[*LineMeta]bool)
    //indexRwLock.RLock()
    //defer indexRwLock.RUnlock()

    keyHi32 := (uint32(keys[0]) << 16) + uint32(keys[1])
    keyLo32 := (uint32(keys[2]) << 16) + uint32(keys[3])

    if len(s.indexHi32[keyHi32]) > 5000 || len(s.indexLo32[keyLo32]) > 5000 {
        mLog.Warn().
            Msgf("large hash slot: %d, %d",
                len(s.indexHi32[keyHi32]), len(s.indexLo32[keyLo32]))
    }

    resChan1 := make(chan LineMetaList, 1)
    resChan2 := make(chan LineMetaList, 1)

    go s.lookup(m, s.indexHi32[keyHi32], resChan1)
    go s.lookup(m, s.indexLo32[keyLo32], resChan2)

    resCount := 0
    for {
        select {
        case r := <- resChan1:
            resCount++
            if len(r) > 0 {
                return r, nil
            }

        case r := <- resChan2:
            resCount++
            if len(r) > 0 {
                return r, nil
            }

        default:
        }

        if resCount == 2 {
            break
        }
    }


    /*
    r, _ := s.lookup(m, s.indexHi32Hi16[keys[0]])
    if len(r) > 0 {
        return r, nil
    }

    r, _ = s.lookup(m, s.indexHi32Lo16[keys[1]])
    if len(r) > 0 {
        return r, nil
    }

    r, _ = s.lookup(m, s.indexLo32Hi16[keys[2]])
    if len(r) > 0 {
        return r, nil
    }

    r, _ = s.lookup(m, s.indexLo32Lo16[keys[3]])
    if len(r) > 0 {
        return r, nil
    }*/

    return nil, nil
}

func (s *SimHashIndex) lookup(m *LineMeta, l LineMetaList, resChan chan LineMetaList) {
    r := make(LineMetaList, 0)
    for _, v := range l {
        //if !resMap[v] && distance(m.SimHash, v.SimHash) <= 3 {
        if distance(m.SimHash, v.SimHash) < dupThreshold {
            r = append(r, v)
            resChan <- r
            return
        }
    }

    resChan <- r
}

func distance(s1 uint64, s2 uint64) uint8 {
    x := (s1 ^ s2) & ((1<<64)-1)
    //mLog.Infof("distance with(s1: %d, s2: %d)", s1, s2)

    var res uint8 = 0
    for x != 0 {
        res += 1
        x &= (x-1)
    }

    return res
}
