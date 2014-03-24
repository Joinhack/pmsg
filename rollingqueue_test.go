package pmsg

import (
	"testing"
)

func TestRollingQueueMetaWriteRead(t *testing.T) {
	var err error
	var q *RollingQueue
	if q, err = createRollingQueue("a.data"); err != nil {
		panic(err)
	}
	q.Push([]byte("121212"))
}
