package id

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	workerIDBits     = 5
	datacenterIDBits = 5
	sequenceBits     = 12

	maxWorkerID     = -1 ^ (-1 << workerIDBits)
	maxDatacenterID = -1 ^ (-1 << datacenterIDBits)
	maxSequence     = -1 ^ (-1 << sequenceBits)

	timeLeft           uint8 = 22
	datacenterLeft           = 17
	workerLeft               = 12
	maxWaitMillisecond       = 100
)

var (
	ErrInvalidWorkerID     = errors.New("invalid worker ID")
	ErrInvalidDatacenterID = errors.New("invalid datacenter ID")
)

type Snowflake struct {
	mu            sync.Mutex
	lastTimestamp int64
	workerID      int64
	datacenterID  int64
	sequence      int64
}

func NewSnowflake(workerID, datacenterID int64) (*Snowflake, error) {
	if workerID < 0 || workerID > maxWorkerID {
		return nil, ErrInvalidWorkerID
	}
	if datacenterID < 0 || datacenterID > maxDatacenterID {
		return nil, ErrInvalidDatacenterID
	}

	return &Snowflake{
		lastTimestamp: 0,
		workerID:      workerID,
		datacenterID:  datacenterID,
		sequence:      0,
	}, nil
}

func (s *Snowflake) tilNextMillis(lastTimestamp int64) int64 {
	timestamp := time.Now().UnixNano() / int64(time.Millisecond)
	for timestamp <= lastTimestamp {
		timestamp = time.Now().UnixNano() / int64(time.Millisecond)
	}
	return timestamp
}

func (s *Snowflake) timeGen() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func (s *Snowflake) NextID() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	timestamp := s.timeGen()
	for s.lastTimestamp > timestamp {
		difference := s.lastTimestamp - timestamp
		if difference > maxWaitMillisecond {
			time.Sleep(time.Duration(maxWaitMillisecond) * time.Millisecond)
		} else {
			waitTime := rand.Int31n(int32(difference))
			time.Sleep(time.Duration(waitTime) * time.Millisecond)
		}
	}
	if s.lastTimestamp <= timestamp {
		s.sequence = (s.sequence + 1) & maxSequence
		if s.sequence == 0 {
			timestamp = s.tilNextMillis(s.lastTimestamp)
		}
	} else {
		s.sequence = 0
	}
	s.lastTimestamp = timestamp
	return (timestamp << timeLeft) | (s.datacenterID << datacenterLeft) | (s.workerID << workerLeft) | s.sequence
}
