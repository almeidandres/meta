package igconnector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

// StageBootstrapHistory stores every selected-thread page before creating its Matrix room.
func (ic *IGClient) StageBootstrapHistory(ctx context.Context, portal *bridgev2.Portal) error {
	meta, err := ic.ensureIGID(ctx, portal)
	if err != nil {
		return err
	}
	job, err := ic.Main.Bridge.DB.GetBootstrapJob(ctx, ic.UserLogin.ID, portal.PortalKey)
	if err != nil {
		return err
	}
	if job != nil && job.SourceComplete {
		return nil
	}
	cursor := ""
	if job != nil {
		cursor = job.Cursor
	}
	for {
		page, err := ic.FetchBootstrapThreadPage(ctx, meta.IGID, cursor)
		if err != nil {
			return err
		}
		items := make([]database.BootstrapItem, 0, len(page.Edges))
		for _, edge := range page.Edges {
			if edge.Node == nil || edge.Node.ID == "" {
				return fmt.Errorf("thread %s returned a message without an ID", meta.IGID)
			}
			raw, err := json.Marshal(edge.Node)
			if err != nil {
				return fmt.Errorf("encode message %s: %w", edge.Node.ID, err)
			}
			items = append(items, database.BootstrapItem{
				StableID: "ig-history:" + edge.Node.ID,
				Kind:     "history",
				Version:  1,
				Payload:  string(raw),
				SourceTS: edge.Node.TimestampMS.UnixMilli(),
			})
		}
		if err = ic.Main.Bridge.DB.StageBootstrapPage(ctx, ic.UserLogin.ID, portal.PortalKey, items, page.PageInfo.EndCursor, !page.PageInfo.HasNextPage); err != nil {
			return fmt.Errorf("persist thread %s history page: %w", meta.IGID, err)
		}
		if !page.PageInfo.HasNextPage {
			return nil
		}
		cursor = page.PageInfo.EndCursor
	}
}

// stageBootstrapDelta persists a selected thread's delta before the socket advances its sequence ID.
func (ic *IGClient) stageBootstrapDelta(ctx context.Context, delta *slidetypes.Delta) (bool, error) {
	threadID := delta.ThreadIGID
	var selected bool
	var timestamp int64
	switch evt := delta.Data.(type) {
	case *slidetypes.NewMessageEvent:
		selected = true
		if evt.Message == nil {
			return false, fmt.Errorf("Instagram message delta has no message")
		}
		if threadID == "" {
			threadID = evt.Message.ThreadFBID
		}
		timestamp = evt.Message.TimestampMS.UnixMilli()
	case *slidetypes.AdminMessageEvent:
		selected = true
		if evt.Message == nil {
			return false, fmt.Errorf("Instagram admin delta has no message")
		}
		if threadID == "" {
			threadID = evt.Message.ThreadFBID
		}
		timestamp = evt.Message.TimestampMS.UnixMilli()
	}
	if threadID == "" {
		return false, nil
	}
	fbid, err := ic.Main.DB.GetFBIDForIGChat(ctx, threadID, ic.UserLogin.ID)
	if err != nil {
		return false, err
	}
	var portal *bridgev2.Portal
	key := ic.makeUncertainPortalKey(fbid)
	if fbid != 0 {
		portal, err = ic.Main.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			return false, err
		}
	}
	selectedJob := false
	if portal == nil && !selected {
		if fbid == 0 {
			return false, nil
		}
		job, err := ic.Main.Bridge.DB.GetBootstrapJob(ctx, ic.UserLogin.ID, key)
		if err != nil {
			return false, err
		}
		if job == nil && !ic.Main.Bridge.Config.SplitPortals {
			key.Receiver = ""
			job, err = ic.Main.Bridge.DB.GetBootstrapJob(ctx, ic.UserLogin.ID, key)
			if err != nil {
				return false, err
			}
		}
		if job == nil || job.Status == "ready" {
			return false, nil
		}
		selectedJob = true
	}
	if portal == nil && !selectedJob {
		resp, err := ic.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(threadID))
		if err != nil {
			return false, err
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.ThreadKey == 0 {
			return false, fmt.Errorf("Instagram thread %s has no usable identity", threadID)
		}
		thread := resp.ThreadInfo.AsIGDirectThread
		if err = ic.saveThreadMappings(ctx, thread); err != nil {
			return false, err
		}
		key = ic.makePortalKey(thread.ThreadKey, thread.IsGroup)
		portal, err = ic.Main.Bridge.GetExistingPortalByKey(ctx, key)
		if err != nil {
			return false, err
		}
	} else if portal != nil {
		key = portal.PortalKey
	}
	if portal != nil && portal.MXID != "" {
		job, err := ic.Main.Bridge.DB.GetBootstrapJob(ctx, ic.UserLogin.ID, key)
		if err != nil {
			return false, err
		}
		if job == nil || job.Status == "ready" {
			return false, nil
		}
	}
	if len(delta.Raw) == 0 {
		return false, fmt.Errorf("Instagram delta has no replayable payload")
	}
	item := database.BootstrapItem{
		StableID: fmt.Sprintf("ig-delta:%d:%x", delta.UQSeqID, sha256.Sum256(delta.Raw)),
		Kind:     "incoming",
		Version:  1,
		Payload:  string(delta.Raw),
		SourceTS: timestamp,
	}
	if err := ic.Main.Bridge.DB.StageBootstrapIncoming(ctx, ic.UserLogin.ID, key, item); err != nil {
		return false, fmt.Errorf("stage Instagram delta before sequence advancement: %w", err)
	}
	if portal == nil {
		if _, err = ic.Main.Bridge.GetPortalByKey(ctx, key); err != nil {
			return false, fmt.Errorf("create selected Instagram portal record: %w", err)
		}
	}
	ic.UserLogin.ResumeBootstrapJobs()
	return true, nil
}

// ReplayBootstrapIncoming uses the same delta handler as the live socket, without re-queuing it.
func (ic *IGClient) ReplayBootstrapIncoming(ctx context.Context, portal *bridgev2.Portal, item database.BootstrapItem) error {
	delta, err := DecodeStagedInstagramDelta(item)
	if err != nil {
		return err
	}
	return ic.handleDeltaWithDispatcher(ctx, delta, func(evt bridgev2.RemoteEvent) bridgev2.EventHandlingResult {
		if err := portal.HandleBootstrapEvent(ctx, ic.UserLogin, evt); err != nil {
			return bridgev2.EventHandlingResultFailed.WithError(err)
		}
		return bridgev2.EventHandlingResultSuccess
	}, true)
}

// DecodeStagedInstagramDelta verifies the durable payload before replay.
func DecodeStagedInstagramDelta(item database.BootstrapItem) (*slidetypes.Delta, error) {
	if item.Kind != "incoming" || item.Version != 1 {
		return nil, fmt.Errorf("unsupported Instagram delta item %s version %d", item.Kind, item.Version)
	}
	var delta slidetypes.Delta
	if err := json.Unmarshal([]byte(item.Payload), &delta); err != nil {
		return nil, fmt.Errorf("decode Instagram delta: %w", err)
	}
	if item.StableID != fmt.Sprintf("ig-delta:%d:%x", delta.UQSeqID, sha256.Sum256(delta.Raw)) {
		return nil, fmt.Errorf("Instagram delta identity changed")
	}
	return &delta, nil
}

func (ic *IGClient) ConvertBootstrapHistory(ctx context.Context, portal *bridgev2.Portal, item database.BootstrapItem) (*bridgev2.BackfillMessage, error) {
	if item.Kind != "history" || item.Version != 1 {
		return nil, fmt.Errorf("unsupported Instagram history item %s version %d", item.Kind, item.Version)
	}
	var msg slidetypes.Message
	if err := json.Unmarshal([]byte(item.Payload), &msg); err != nil {
		return nil, fmt.Errorf("decode Instagram history item: %w", err)
	}
	if msg.ID == "" || item.StableID != "ig-history:"+msg.ID {
		return nil, fmt.Errorf("Instagram history item identity changed")
	}
	converted, err := ic.wrapBackfillMessages(ctx, portal, []slidetypes.Node[*slidetypes.Message]{{Node: &msg}})
	if err != nil {
		return nil, err
	}
	if len(converted) != 1 || converted[0] == nil || converted[0].ConvertedMessage == nil || len(converted[0].Parts) == 0 {
		return nil, fmt.Errorf("failed to convert Instagram history item %s", msg.ID)
	}
	return converted[0], nil
}

var _ bridgev2.PortalBootstrapSource = (*IGClient)(nil)
