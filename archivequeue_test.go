package pmsg

import (
	"fmt"
	"testing"
)

func TestRollingQueueMetaWriteRead(t *testing.T) {
	var err error
	var q *RollingQueue
	if q, err = createRollingQueue("a.data"); err != nil {
		panic(err)
	}

	for i := 0; i < 10000000; i++ {
		q.Push([]byte(fmt.Sprintf("%d:121212121212121212121212", i)))
	}
}
