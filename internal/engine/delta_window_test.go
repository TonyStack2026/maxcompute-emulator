package engine

import (
	"context"
	"fmt"
	"testing"
)

// TestDeltaReadFutureEndStaysInsideCommittedData covers the review finding on the
// version window: DeltaRead computed the committed bound but only used it for a
// negative end, then handed back the caller's end. A consumer that asked for (0,100]
// on a table at version 2 got LatestVersion=100, resumed from 100 afterwards, and lost
// every version between the bound and its own number - permanently, because the cursor
// is what the consumer persists.
//
// The window a read consumes and the cursor it returns are therefore both capped at the
// committed bound. The refusal code a live service uses for an over-future start is not
// verified, so only the "no silent cursor advance" half is pinned here.
func TestDeltaReadFutureEndStaysInsideCommittedData(t *testing.T) {
	ctx := context.Background()
	e := openTestEngine(t)
	if _, err := e.Execute(ctx, "p", "default",
		"create table fw(id bigint, s string, primary key(id)) tblproperties('transactional'='true')"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(ctx, "p", "default", "insert into fw values (1,'a'),(2,'b')"); err != nil {
		t.Fatal(err)
	}
	res, cursor, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 0, 100, true)
	if err != nil {
		t.Fatalf("future window: %v", err)
	}
	if cursor != 2 {
		t.Fatalf("future window cursor=%d want the committed bound 2 (rows=%d)", cursor, len(res.Rows))
	}
	if got := fmt.Sprint(opCodes(res)); got != "[2 2]" {
		t.Fatalf("future window events=%s want the two committed inserts", got)
	}
	// The point of the cap: the version committed after that read is still ahead of the
	// cursor the consumer stored, so resuming from it must find it.
	if _, err = e.Execute(ctx, "p", "default", "insert into fw values (3,'c')"); err != nil {
		t.Fatal(err)
	}
	next, after, err := e.DeltaRead(ctx, "p", "default", "fw", nil, cursor, -1, true)
	if err != nil {
		t.Fatalf("resume from the capped cursor: %v", err)
	}
	if got := fmt.Sprint(opCodes(next)); got != "[2]" {
		t.Fatalf("events after the future window=%s want the version-3 insert the consumer must not lose", got)
	}
	if after != 3 {
		t.Fatalf("cursor after resume=%d want 3", after)
	}
	// A window that starts above the committed bound cannot be served: returning the
	// caller's end there would be the same cursor corruption in a different shape, so it
	// fails the same way an inverted range does.
	if _, _, err = e.DeltaRead(ctx, "p", "default", "fw", nil, 5, 100, true); err == nil {
		t.Fatal("a start above the committed bound was accepted; a read past the bound must not be silent")
	}
	// A negative end still means "up to now" and is unaffected by the cap.
	if _, open, err := e.DeltaRead(ctx, "p", "default", "fw", nil, 0, -1, true); err != nil || open != 3 {
		t.Fatalf("open-ended read cursor=%d err=%v want 3", open, err)
	}
}
