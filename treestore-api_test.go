package treestore_client

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jimsnab/go-lane"
)

func TestSetGet(t *testing.T) {
	l := lane.NewTestingLane(context.Background())
	tsc := NewTSClient(l)

	tick := fmt.Sprintf("%d", time.Now().UnixNano())

	_, _, err := tsc.SetKeyValue(MakeStoreKey("client", "test", "key"), tick)
	if err != nil {
		t.Fatal(err)
	}

	value, ke, vs, err := tsc.GetKeyValue(MakeStoreKey("client", "test", "key"))
	if err != nil {
		t.Fatal(err)
	}

	if string(value.([]byte)) != tick {
		t.Error("not check")
	}
	if !ke || !vs {
		t.Error("wrong flags")
	}
}