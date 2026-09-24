package igconnector

import (
	"testing"

	"go.mau.fi/mautrix-meta/pkg/instameow/slidetypes"
)

func TestPaginationNeedsAdvancingCursor(t *testing.T) {
	for _, tc := range []struct {
		name, next string
		hasMore    bool
		wantError  bool
	}{
		{"missing cursor", "", true, true},
		{"unchanged cursor", "old", true, true},
		{"empty page with next cursor", "new", true, false},
		{"final page", "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			page := &slidetypes.SlideMessages{PageInfo: slidetypes.PageInfo{EndCursor: tc.next, HasNextPage: tc.hasMore}}
			if (checkMessageCursor("old", page) != nil) != tc.wantError {
				t.Fatalf("next=%q hasMore=%t: unexpected cursor validation", tc.next, tc.hasMore)
			}
		})
	}
}
