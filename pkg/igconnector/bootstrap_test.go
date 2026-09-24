package igconnector

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"

	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func TestStagedDeltaRoundTrip(t *testing.T) {
	raw := []byte(`{"__typename":"SlideUQPPNewMessage","uq_seq_id":"23","thread_fbid":"thread","message":{"id":"message"}}`)
	var source slidetypes.Delta
	if err := json.Unmarshal(raw, &source); err != nil {
		t.Fatal(err)
	}
	item := database.BootstrapItem{
		StableID: fmt.Sprintf("ig-delta:%d:%x", source.UQSeqID, sha256.Sum256(source.Raw)),
		Kind:     "incoming", Version: 1, Payload: string(source.Raw),
	}
	decoded, err := DecodeStagedInstagramDelta(item)
	if err != nil {
		t.Fatal(err)
	}
	message, ok := decoded.Data.(*slidetypes.NewMessageEvent)
	if !ok || message.Message == nil || message.Message.ID != "message" || decoded.ThreadIGID != "thread" {
		t.Fatalf("delta lost message or thread: %+v", decoded)
	}
	item.Payload = `{"__typename":"SlideUQPPNewMessage","uq_seq_id":"23","thread_fbid":"other","message":{"id":"message"}}`
	if _, err = DecodeStagedInstagramDelta(item); err == nil {
		t.Fatal("changed delta accepted for saved identity")
	}
}
