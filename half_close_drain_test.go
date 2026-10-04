package smux

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func halfCloseDrainPair(t *testing.T, version int) (*Session, *Session) {
	t.Helper()
	ln, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })

	config := DefaultConfig()
	config.Version = version

	serverCh := make(chan *Session, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			serverCh <- nil
			return
		}
		sess, _ := Server(conn, config)
		serverCh <- sess
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	client, err := Client(conn, config)
	if err != nil {
		t.Fatal(err)
	}
	server := <-serverCh
	if server == nil {
		t.Fatal("server session not created")
	}
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client, server
}

// bothHalvesClosedWithTail returns stream A after A sent its FIN and then
// processed B's "tail" followed by B's FIN.
func bothHalvesClosedWithTail(t *testing.T, client, server *Session) *Stream {
	t.Helper()
	a, err := client.OpenStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}
	b, err := server.AcceptStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadFull(b, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}

	if err := a.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Write([]byte("tail")); err != nil {
		t.Fatal(err)
	}
	if err := b.CloseWrite(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-a.chFinEvent:
	case <-time.After(5 * time.Second):
		t.Fatal("peer FIN was not processed")
	}
	return a
}

func waitStreamsReleased(t *testing.T, sess *Session) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		streams := sess.NumStreams()
		bucket := atomic.LoadInt32(&sess.bucket)
		if streams == 0 && bucket == int32(sess.config.MaxReceiveBuffer) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("not released: streams=%d bucket=%d want 0 and %d",
				streams, bucket, sess.config.MaxReceiveBuffer)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestBothHalvesClosedKeepsUnreadPeerData(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			a := bothHalvesClosedWithTail(t, client, server)

			got, err := io.ReadAll(a)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(got) != "tail" {
				t.Fatalf("ReadAll = %q, want %q", got, "tail")
			}
		})
	}
}

func TestBothHalvesClosedReleasesTheStreamAfterTheDrain(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version)+"/drained", func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			a := bothHalvesClosedWithTail(t, client, server)

			if _, err := io.ReadAll(a); err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			waitStreamsReleased(t, client)
			waitStreamsReleased(t, server)
		})
		t.Run(versionName(version)+"/closed", func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			a := bothHalvesClosedWithTail(t, client, server)

			a.Close()
			waitStreamsReleased(t, client)
			waitStreamsReleased(t, server)
		})
	}
}

func versionName(version int) string {
	if version == 2 {
		return "v2"
	}
	return "v1"
}

func TestBothHalvesClosedRetainedStreamCountsLikeAnOpenStream(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			bothHalvesClosedWithTail(t, client, server)

			if n := client.NumStreams(); n != 1 {
				t.Fatalf("NumStreams = %d, want 1", n)
			}
			want := int32(client.config.MaxReceiveBuffer - len("tail"))
			if got := atomic.LoadInt32(&client.bucket); got != want {
				t.Fatalf("bucket = %d, want %d", got, want)
			}
		})
	}
}

func TestSessionCloseKeepsRetainedStreamsReadable(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			a := bothHalvesClosedWithTail(t, client, server)

			client.Close()
			got, err := io.ReadAll(a)
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(got) != "tail" {
				t.Fatalf("ReadAll = %q, want %q", got, "tail")
			}
			if n := client.NumStreams(); n != 0 {
				t.Fatalf("NumStreams = %d, want 0", n)
			}
			want := int32(client.config.MaxReceiveBuffer)
			if got := atomic.LoadInt32(&client.bucket); got != want {
				t.Fatalf("bucket = %d, want %d", got, want)
			}
		})
	}
}

func TestBothHalvesClosedWriteToKeepsTailAndReleases(t *testing.T) {
	for _, version := range []int{1, 2} {
		version := version
		t.Run(versionName(version), func(t *testing.T) {
			client, server := halfCloseDrainPair(t, version)
			a := bothHalvesClosedWithTail(t, client, server)

			var buf bytes.Buffer
			if _, err := a.WriteTo(&buf); err != nil && err != io.EOF {
				t.Fatalf("WriteTo: %v", err)
			}
			if buf.String() != "tail" {
				t.Fatalf("WriteTo wrote %q, want %q", buf.String(), "tail")
			}
			waitStreamsReleased(t, client)
			waitStreamsReleased(t, server)
		})
	}
}
