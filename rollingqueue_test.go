package pmsg

import (
	"fmt"
	"testing"
	"time"
)

func TestRollingQueueMetaWriteRead(t *testing.T) {
	var err error
	var q *RollingQueue
	if q, err = createRollingQueue("a.data"); err != nil {
		panic(err)
	}

	go func() {
		for i := 0; i < 10000000; i++ {
			q.Push([]byte(fmt.Sprintf("%d:121212121212121212121212", i)))
		}
	}()
	go func() {
		var i int = 0
		for {

			q.Pop()
			i++
			if i == 10000000-1 {
				println("finish")
			}
			//println(string(q.Pop()))
			//println(q.WriteBytes, q.ReadBytes)
		}
	}()
	time.Sleep(30 * time.Hour)
}
