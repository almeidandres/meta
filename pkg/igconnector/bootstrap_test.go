package igconnector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/zerolog"
	"go.mau.fi/util/dbutil"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/bridgeconfig"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

type bootstrapIGMatrixTest struct{ bridgev2.MatrixConnector }

func (bootstrapIGMatrixTest) Init(*bridgev2.Bridge)         {}
func (bootstrapIGMatrixTest) BotIntent() bridgev2.MatrixAPI { return nil }

func TestSelectedInstagramDeltaBeforePortalRecord(t *testing.T) {
	ctx := context.Background()
	raw, err := dbutil.NewWithDialect("file:"+filepath.Join(t.TempDir(), "incoming.db")+"?_foreign_keys=on", "sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	connector := &IGConnector{}
	bridge := bridgev2.NewBridge("ig", raw, zerolog.Nop(), &bridgeconfig.BridgeConfig{}, bootstrapIGMatrixTest{}, connector, func(*bridgev2.Bridge) bridgev2.CommandProcessor { return nil })
	bridge.BackgroundCtx = ctx
	if err = bridge.DB.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	meta := connector.DB
	if err = meta.Upgrade(ctx); err != nil {
		t.Fatal(err)
	}
	if err = bridge.DB.User.Insert(ctx, &database.User{MXID: "@owner:localhost"}); err != nil {
		t.Fatal(err)
	}
	login := &bridgev2.UserLogin{UserLogin: &database.UserLogin{ID: "login", UserMXID: "@owner:localhost"}, Bridge: bridge}
	if err = bridge.DB.UserLogin.Insert(ctx, login.UserLogin); err != nil {
		t.Fatal(err)
	}
	ic := &IGClient{Main: connector, UserLogin: login}
	for _, receiver := range []networkid.UserLoginID{"login", ""} {
		threadID := "thread-" + string(receiver)
		fbid := int64(123)
		if receiver == "" {
			fbid = 456
		}
		if err = meta.PutFBIDForIGChat(ctx, threadID, fbid, login.ID); err != nil {
			t.Fatal(err)
		}
		key := networkid.PortalKey{ID: ic.makeUncertainPortalKey(fbid).ID, Receiver: receiver}
		if err = bridge.DB.EnsureBootstrapJob(ctx, login.ID, key); err != nil {
			t.Fatal(err)
		}
		rawDelta := []byte(fmt.Sprintf(`{"__typename":"SlideUQPPCreateReaction","uq_seq_id":"23","thread_fbid":%q,"message_id":"target"}`, threadID))
		var delta slidetypes.Delta
		if err = json.Unmarshal(rawDelta, &delta); err != nil {
			t.Fatal(err)
		}
		staged, err := ic.stageBootstrapDelta(ctx, &delta)
		if err != nil || !staged {
			t.Fatalf("selected thread lost reaction before portal record (receiver %q): staged=%v err=%v", receiver, staged, err)
		}
		items, err := bridge.DB.GetPendingBootstrapItems(ctx, login.ID, key, 10)
		if err != nil || len(items) != 1 || items[0].Kind != "incoming" {
			t.Fatalf("selected reaction was not durable: %+v %v", items, err)
		}
		if _, err = DecodeStagedInstagramDelta(items[0]); err != nil {
			t.Fatalf("staged reaction could not be replayed: %v", err)
		}
	}
}

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
