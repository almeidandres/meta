package igconnector

import (
	"context"
	"testing"
)

func TestMatrixReadStateStaysLocal(t *testing.T) {
	client := &IGClient{}
	if err := client.HandleMatrixReadReceipt(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}
