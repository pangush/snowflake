package snowflake

import (
	"fmt"
	"log"
	"sync"
	"time"
)

/**
* twitter snowflake
* 0 - 000 0000 0000 0000 0000 0000 0000 0000 0000 0000 00 - 00 000 - 0 0000 - 0000 0000 0000
* 1位 标识符
* 41位 时间戳（毫秒级）。当前时间减起始时间的值，可以使用69年。
* 10位 5位datacenterId和5位workerId(10位的长度最多支持部署1024个节点）。
* 12位 毫秒级内的序列。支持每个节点每毫秒产生4096个序列
*/
const (
	twepoch = int64(1577808000000) // 设置起始时间(时间戳/毫秒)：2020-01-01 00:00:00，有效期69年

	workerIdBits = 5 // 机器id所占位数
	datacenterIdBits = 5 // 数据id所占位数
	maxWorkerId = -1 ^ (-1 << workerIdBits) // 机器id最大值
	maxDatacenterId = -1 ^ (-1 << datacenterIdBits) // 数据id最大值
	sequenceBits = 12 // 毫秒内序列所占位数

	workerIdShift = sequenceBits // 机器id左移位数
	datacenterIdShift = sequenceBits + workerIdBits // 数据id左移位数
	timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits // 时间戳左移位数
	sequenceMask = -1 ^ (-1 << sequenceBits) // 毫秒内序列最大值
)

type Snowflake struct {
	mu 				sync.Mutex
	lastTimestamp	int64
	workerId     	int64
	datacenterId 	int64
	sequence     	int64
}

func New(workerId int64, datacenterId int64) (*Snowflake, error) {
	if workerId < 0 || workerId > maxWorkerId {
		return nil, fmt.Errorf("worker Id can't be greater than %d or less than 0", maxWorkerId)
	}
	if datacenterId < 0 || datacenterId > maxDatacenterId {
		return nil, fmt.Errorf("datacenter Id can't be greater than %d or less than 0", datacenterId)
	}

	log.Printf("worker starting. timestamp left shift %d, datacenter id bits %d, worker id bits %d, sequence bits %d, workerid %d",
		timestampLeftShift, datacenterIdBits, workerIdBits, sequenceBits, workerId)

	return &Snowflake{
		lastTimestamp: 0,
		workerId:      workerId,
		datacenterId:  datacenterId,
		sequence:      0,
	}, nil
}

func (s *Snowflake) NextId() (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	timestamp := timeGen()

	// 当前时间戳小于上一次ID生成的时间戳，说明系统时钟回退过，这个时候应当抛出异常
	if timestamp < s.lastTimestamp {
		//log.Printf("clock is moving backwards.  Rejecting requests until %d.", s.lastTimestamp)
		return 0, fmt.Errorf("Clock moved backwards.  Refusing to generate id for %d milliseconds", s.lastTimestamp - timestamp)
	}

	// 如果是同一时间生成的，则进行毫秒内序列
	if timestamp == s.lastTimestamp {
		s.sequence = (s.sequence + 1) & sequenceMask
		if s.sequence == 0 { // 序列用尽
			timestamp = tilNextMillis(s.lastTimestamp)
		}
	} else {
		s.sequence = 0
	}

	s.lastTimestamp = timestamp
	return ((timestamp - twepoch) << timestampLeftShift) |
		(s.datacenterId << datacenterIdShift) |
		(s.workerId << workerIdShift) |
		s.sequence, nil
}

// 获取当前时间戳(毫秒级)
func timeGen() int64 {
	return time.Now().UnixNano() / 1e6
}

// 阻塞到下一个毫秒，直到获得新的时间戳
func tilNextMillis(lastTimestamp int64) int64 {
	timestamp := timeGen()
	for timestamp <= lastTimestamp {
		timestamp = timeGen()
	}
	return timestamp
}
