package pmsg

import (
	"testing"
)

func TestRollingQueueMetaWriteRead(t *testing.T) {
	createRollingQueue("a.data")
}
