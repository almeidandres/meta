// mautrix-meta - A Matrix-Facebook Messenger and Instagram DM puppeting bridge.
// Copyright (C) 2026 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package igconnector

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/networkid"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
	"go.mau.fi/mautrix-meta/pkg/metadb"
	"go.mau.fi/mautrix-meta/pkg/metaid"
)

var _ bridgev2.BackfillingNetworkAPI = (*IGClient)(nil)

func (ic *IGClient) FetchMessages(ctx context.Context, params bridgev2.FetchMessagesParams) (*bridgev2.FetchMessagesResponse, error) {
	meta, err := ic.ensureIGID(ctx, params.Portal)
	if err != nil {
		return nil, err
	}
	if params.Forward {
		return ic.fetchForwardBackfill(ctx, params, meta)
	}
	return ic.fetchBackwardBackfill(ctx, params, meta)
}

func (ic *IGClient) fetchForwardBackfill(ctx context.Context, params bridgev2.FetchMessagesParams, meta *metaid.PortalMetadata) (*bridgev2.FetchMessagesResponse, error) {
	thread, ok := params.BundledData.(*slidetypes.ThreadInfo)
	if !ok {
		zerolog.Ctx(ctx).Debug().Msg("Fetching thread for forward backfill request...")
		resp, err := ic.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(meta.IGID))
		if err != nil {
			return nil, fmt.Errorf("failed to get thread info: %w", err)
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.SlideMessages == nil {
			return nil, fmt.Errorf("thread %s has no messages page", meta.IGID)
		}
		zerolog.Ctx(ctx).Trace().
			Any("thread_response", resp.ThreadInfo.AsIGDirectThread).
			Msg("Response for initial thread fetch")
		thread = resp.ThreadInfo.AsIGDirectThread
		params.Portal.UpdateInfo(ctx, ic.wrapChatInfo(thread), ic.UserLogin, nil, time.Time{})
	}
	if thread == nil || thread.SlideMessages == nil {
		return nil, fmt.Errorf("thread info has no messages page")
	}
	canBackwardsBackfill := params.AnchorMessage == nil && ic.Main.Bridge.Config.Backfill.Queue.AnyEnabled()
	cursor := thread.SlideMessages.PageInfo.EndCursor
	var anchorID string
	var anchorTS time.Time
	if params.AnchorMessage != nil {
		rawParsed := metaid.ParseMessageID(params.AnchorMessage.ID)
		parsed, ok := rawParsed.(metaid.ParsedFBMessageID)
		if !ok {
			return nil, fmt.Errorf("unexpected message ID type %T", rawParsed)
		}
		anchorID = parsed.ID
		anchorTS = params.AnchorMessage.Timestamp
	}
	foundAnchor := false
	var messages []slidetypes.Node[*slidetypes.Message]
	appendMessages := func(t *slidetypes.SlideMessages) {
		if anchorID != "" {
			for i, msg := range t.Edges {
				if msg.Node.ID == anchorID || msg.Node.TimestampMS.Before(anchorTS) {
					foundAnchor = true
					t.Edges = t.Edges[:i]
					break
				}
			}
		}
		messages = append(messages, t.Edges...)
	}
	appendMessages(thread.SlideMessages)
	page := thread.SlideMessages
	for len(messages) < params.Count && page.PageInfo.HasNextPage && !foundAnchor && !canBackwardsBackfill {
		if cursor == "" {
			return nil, fmt.Errorf("thread %s has more messages but no cursor", meta.IGID)
		}
		zerolog.Ctx(ctx).Debug().
			Int("collected_count", len(messages)).
			Int("limit", params.Count).
			Str("cursor", cursor).
			Msg("Fetching additional batch of messages for forward backfill")
		resp, err := ic.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			AfterCursor: &cursor,
			ThreadID:    meta.IGID,
			FirstN:      20,

			InitialMessagePageCount: 20,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to paginate messages: %w", err)
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.Messages == nil {
			return nil, fmt.Errorf("thread %s pagination returned no messages page", meta.IGID)
		}
		zerolog.Ctx(ctx).Trace().
			Any("paginate_response", resp.ThreadInfo.AsIGDirectThread.Messages).
			Msg("Response for pagination")
		page = resp.ThreadInfo.AsIGDirectThread.Messages
		if err = checkMessageCursor(cursor, page); err != nil {
			return nil, fmt.Errorf("thread %s pagination: %w", meta.IGID, err)
		}
		appendMessages(page)
		cursor = page.PageInfo.EndCursor
	}
	var markRead bool
	for _, rr := range thread.SlideReadReceipts {
		if metaid.MakeUserLoginID(rr.ParticipantFBID) == ic.UserLogin.ID {
			markRead = !rr.WatermarkTimestampMS.Before(thread.LastActivityTimestampMS.Time)
			break
		}
	}
	if thread.MarkedAsUnread {
		markRead = false
	}
	converted, err := ic.wrapBackfillMessages(ctx, params.Portal, messages)
	if err != nil {
		return nil, err
	}
	return &bridgev2.FetchMessagesResponse{
		Messages: converted,
		Forward:  true,
		MarkRead: markRead,
	}, ctx.Err()
}

const BackfillCursorPrefix = "ig:"

func checkMessageCursor(previous string, page *slidetypes.SlideMessages) error {
	if page.PageInfo.HasNextPage && (page.PageInfo.EndCursor == "" || page.PageInfo.EndCursor == previous) {
		return fmt.Errorf("more messages reported without a new cursor")
	}
	return nil
}

// FetchBootstrapThreadPage returns one source page without creating a Matrix room.
// The caller must persist the page before requesting the returned cursor.
func (ic *IGClient) FetchBootstrapThreadPage(ctx context.Context, threadIGID, cursor string) (*slidetypes.SlideMessages, error) {
	if threadIGID == "" {
		return nil, fmt.Errorf("thread ID is required")
	}
	var page *slidetypes.SlideMessages
	if cursor == "" {
		resp, err := ic.Client.GetThread(ctx, slidetypes.MakeGetThreadInfoRequest(threadIGID))
		if err != nil {
			return nil, fmt.Errorf("get thread %s: %w", threadIGID, err)
		}
		if resp != nil && resp.ThreadInfo.AsIGDirectThread != nil {
			page = resp.ThreadInfo.AsIGDirectThread.SlideMessages
		}
	} else {
		resp, err := ic.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			AfterCursor:             &cursor,
			ThreadID:                threadIGID,
			FirstN:                  20,
			InitialMessagePageCount: 20,
		})
		if err != nil {
			return nil, fmt.Errorf("paginate thread %s: %w", threadIGID, err)
		}
		if resp != nil && resp.ThreadInfo.AsIGDirectThread != nil {
			page = resp.ThreadInfo.AsIGDirectThread.Messages
		}
	}
	if page == nil {
		return nil, fmt.Errorf("thread %s returned no messages page", threadIGID)
	}
	if err := checkMessageCursor(cursor, page); err != nil {
		return nil, fmt.Errorf("thread %s: %w", threadIGID, err)
	}
	return page, nil
}

func (ic *IGClient) fetchBackwardBackfill(ctx context.Context, params bridgev2.FetchMessagesParams, meta *metaid.PortalMetadata) (*bridgev2.FetchMessagesResponse, error) {
	cursorVal, ok := strings.CutPrefix(string(params.Cursor), BackfillCursorPrefix)
	var beforeMessageID *string
	cursor := &cursorVal
	if !ok || cursorVal == "" {
		cursor = nil
		if params.AnchorMessage == nil {
			// TODO is it possible to hit this for non-empty chats?
			zerolog.Ctx(ctx).Debug().Msg("Returning empty response to backwards backfill request with no cursor nor anchor")
			return &bridgev2.FetchMessagesResponse{}, nil
		}
		rawParsed := metaid.ParseMessageID(params.AnchorMessage.ID)
		parsed, ok := rawParsed.(metaid.ParsedFBMessageID)
		if !ok {
			return nil, fmt.Errorf("unexpected message ID type %T", rawParsed)
		}
		beforeMessageID = &parsed.ID
	}
	hasMore := true
	var messages []slidetypes.Node[*slidetypes.Message]
	for hasMore && len(messages) < params.Count {
		resp, err := ic.Client.PaginateMessages(ctx, &slidetypes.PaginateMessagesRequest{
			AfterCursor:        cursor,
			OlderThanMessageID: beforeMessageID,
			ThreadID:           meta.IGID,
			FirstN:             20,

			InitialMessagePageCount: 20,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to paginate messages: %w", err)
		}
		if resp == nil || resp.ThreadInfo.AsIGDirectThread == nil || resp.ThreadInfo.AsIGDirectThread.Messages == nil {
			return nil, fmt.Errorf("thread %s pagination returned no messages page", meta.IGID)
		}
		zerolog.Ctx(ctx).Trace().
			Any("paginate_response", resp.ThreadInfo.AsIGDirectThread.Messages).
			Msg("Response for backwards pagination")
		page := resp.ThreadInfo.AsIGDirectThread.Messages
		previous := ""
		if cursor != nil {
			previous = *cursor
		}
		if err = checkMessageCursor(previous, page); err != nil {
			return nil, fmt.Errorf("thread %s pagination: %w", meta.IGID, err)
		}
		messages = append(messages, resp.ThreadInfo.AsIGDirectThread.Messages.Edges...)
		beforeMessageID = nil
		cursor = &resp.ThreadInfo.AsIGDirectThread.Messages.PageInfo.EndCursor
		hasMore = resp.ThreadInfo.AsIGDirectThread.Messages.PageInfo.HasNextPage
	}
	if len(messages) == 0 || cursor == nil {
		return &bridgev2.FetchMessagesResponse{}, nil
	}
	converted, err := ic.wrapBackfillMessages(ctx, params.Portal, messages)
	if err != nil {
		return nil, err
	}
	return &bridgev2.FetchMessagesResponse{
		Messages: converted,
		Cursor:   networkid.PaginationCursor(BackfillCursorPrefix + *cursor),
		HasMore:  hasMore,
	}, ctx.Err()
}

func (ic *IGClient) wrapBackfillMessages(
	ctx context.Context, portal *bridgev2.Portal, messages []slidetypes.Node[*slidetypes.Message],
) ([]*bridgev2.BackfillMessage, error) {
	// Instagram returns messages newest to oldest, bridgev2 wants oldest to newest
	slices.Reverse(messages)
	out := make([]*bridgev2.BackfillMessage, len(messages))
	var reactions []*metadb.IGReactionEntry
	for i, msg := range messages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if msg.Node == nil {
			return nil, fmt.Errorf("Instagram history page contains an empty message")
		}
		msgID := metaid.MakeFBMessageID(msg.Node.ID)
		sender := ic.makeEventSender(msg.Node.SenderFBID)
		intent, ok := portal.GetIntentFor(ctx, sender, ic.UserLogin, bridgev2.RemoteEventBackfill)
		if !ok {
			intent = ic.Main.Bridge.Bot
		}
		out[i] = &bridgev2.BackfillMessage{
			ConvertedMessage: ic.Main.MsgConv.ToMatrix(
				ctx, portal, ic.Client, ic.UserLogin, intent, msgID, msg.Node,
				ic.Main.Config.DisableXMABackfill || ic.Main.Config.DisableXMAAlways,
			),
			Sender:      sender,
			ID:          msgID,
			Timestamp:   msg.Node.TimestampMS.Time,
			StreamOrder: msg.Node.TimestampMS.UnixMilli(),
			Reactions:   make([]*bridgev2.BackfillReaction, len(msg.Node.Reactions)),
			//TxnID: msg.Node.OfflineThreadingID,
		}
		for j, react := range msg.Node.Reactions {
			out[i].Reactions[j] = &bridgev2.BackfillReaction{
				Timestamp: react.ReactionTimestampMS.Time,
				Sender:    ic.makeEventSender(react.SenderFBID),
				Emoji:     react.Reaction,
			}
			if react.LogMessageID != "" {
				reactions = append(reactions, &metadb.IGReactionEntry{
					TargetMsgID:   msg.Node.ID,
					Sender:        react.SenderFBID,
					ReactionMsgID: react.LogMessageID,
				})
			}
		}
	}
	if err := ic.Main.DB.PutManyIGReactions(ctx, portal.PortalKey, reactions); err != nil {
		return nil, fmt.Errorf("store Instagram reaction mappings: %w", err)
	}
	return out, nil
}
