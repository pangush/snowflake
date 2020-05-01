package snowflake

import (
	"fmt"
	"testing"
	"time"
)

func TestSnowflake_NextId(t *testing.T) {
	sf, err := New(int64(0), int64(0))
	if err != nil {
		t.Error(err)
	}

	startTime := time.Now().UnixNano()
	maxCount := 10000000
	for i:=0; i<maxCount; i++ {
		_, err := sf.NextId()
		if err != nil {
			t.Error(err)
		}
	}
	fmt.Printf("generate id count = %v cost  %v s", maxCount, (time.Now().UnixNano() - startTime) / 1e9)
}