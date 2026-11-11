package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

// TestStorageIncrementalFutureEndKeepsTheCursorInsideCommittedData covers the same review
// finding through both public surfaces a consumer actually uses: the v1 session_type form
// and the storage-api v2 form. Both hand the consumer LatestVersion as the version to
// resume from, so an over-future EndVersion echoed back as the cursor is how a real
// consumer loses data - it stores 100, the table was at 2, and version 3 lands behind its
// cursor for good.
func TestStorageIncrementalFutureEndKeepsTheCursorInsideCommittedData(t *testing.T) {
	s, h := fixture(t, Config{})
	createAcidTable(t, s, "acidf", 1, 2)
	body := func(from, to int64) string {
		return `{"IncrementalReadOptions":{"StartVersion":` + strconv.FormatInt(from, 10) +
			`,"EndVersion":` + strconv.FormatInt(to, 10) + `,"Mode":"cdc"}}`
	}
	code, first, sid := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(0, 100))
	if code != http.StatusOK || sid == "" {
		t.Fatalf("v1 future window: code=%d body=%v", code, first)
	}
	if v := int64(first["LatestVersion"].(float64)); v != 2 {
		t.Fatalf("v1 future window LatestVersion=%d want the committed bound 2", v)
	}
	if v := int64(first["RecordCount"].(float64)); v != 2 {
		t.Fatalf("v1 future window RecordCount=%d want the two committed events", v)
	}
	// A version committed after that session is still ahead of the stored cursor, so the
	// next window from that cursor finds it.
	insertAcidRows(t, s, "acidf", 3, 4)
	code, second, sid2 := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"),
		body(int64(first["LatestVersion"].(float64)), 100))
	if code != http.StatusOK || sid2 == "" {
		t.Fatalf("v1 resume: code=%d body=%v", code, second)
	}
	if v := int64(second["RecordCount"].(float64)); v != 2 {
		t.Fatalf("v1 resume RecordCount=%d want the two versions committed after the future window", v)
	}
	if _, ops := readEvents(t, h, "acidf", sid2); len(ops) != 2 {
		t.Fatalf("v1 resume events=%v want two", ops)
	}
	// Same numbers on the storage-api v2 surface.
	code, v2, v2sid := openSession(t, h,
		"/api/storage/v2?Action=TableCreateReadSession&Target=projects.p.schemas.default.tables.acidf",
		`{"IncrementalRead":true,"IncrementalReadOptions":{"StartVersion":0,"EndVersion":100,"Mode":"cdc"}}`)
	if code != http.StatusOK || v2sid == "" {
		t.Fatalf("v2 future window: code=%d body=%v", code, v2)
	}
	if v := int64(v2["LatestVersion"].(float64)); v != 4 {
		t.Fatalf("v2 future window LatestVersion=%d want the committed bound 4", v)
	}
	if echoed, ok := v2["IncrementalReadOptions"].(map[string]any); !ok || int64(echoed["EndVersion"].(float64)) != 4 {
		t.Fatalf("v2 echoed window=%v want the clamped end 4, not the requested 100", v2["IncrementalReadOptions"])
	}
	// A start above the committed bound cannot be served: it fails as an invalid value
	// (400, same family as the pinned inverted range) instead of returning an empty window
	// with a cursor the consumer must not store.
	code, resp, sidBad := openSession(t, h, v1Path("acidf", "/sessions?session_type=incremental_read"), body(50, 100))
	if code != http.StatusBadRequest || sidBad != "" {
		t.Fatalf("v1 over-future start: code=%d want 400 body=%v", code, resp)
	}
	if !strings.Contains(fmt.Sprint(resp), "InvalidParameter") {
		t.Fatalf("v1 over-future start code=%d body=%v want the InvalidParameter family", code, resp)
	}
}
