package transport

import (
	"github.com/docker/swarmkit/log"
	"math"
	"testing"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/assert"
)

func TestSplitSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var raftMsg raftpb.Message
	raftMsg.Type = raftpb.MsgSnap
	snaphotSize := 8 << 20
	raftMsg.Snapshot.Data = make([]byte, snaphotSize)

	raftMessagePayloadSize := raftMessagePayloadSize(&raftMsg)
	log.G(ctx).Infof("test payload size: %d", raftMessagePayloadSize)

	numMsgs := int(math.Ceil(float64(snaphotSize) / float64(raftMessagePayloadSize)))
	msgs := splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, numMsgs, len(msgs), "Unexpected number of messages")

	raftMsg.Snapshot.Data = make([]byte, raftMessagePayloadSize)
	msgs = splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, 1, len(msgs), "Unexpected number of messages")

	raftMsg.Snapshot.Data = make([]byte, raftMessagePayloadSize-1)
	msgs = splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, 1, len(msgs), "Unexpected number of messages")

	raftMsg.Snapshot.Data = make([]byte, raftMessagePayloadSize*2)
	msgs = splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, 2, len(msgs), "Unexpected number of messages")

	raftMsg.Snapshot.Data = make([]byte, 0)
	msgs = splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, len(msgs), 0, "Unexpected number of messages")

	raftMsg.Type = raftpb.MsgApp
	msgs = splitSnapshotData(ctx, &raftMsg)
	assert.Equal(t, len(msgs), 0, "Unexpected number of messages")
}
