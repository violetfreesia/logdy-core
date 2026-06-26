package http

import (
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/logdyhq/logdy-core/models"

	"github.com/stretchr/testify/assert"
)

func readMessages(t *testing.T, ch <-chan []Message) []Message {
	t.Helper()

	select {
	case msg := <-ch:
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for client messages")
	}
	return nil
}

func readMessagesUntil(t *testing.T, ch <-chan []Message, count int) []Message {
	t.Helper()

	msgs := []Message{}
	deadline := time.After(time.Second)
	for len(msgs) < count {
		select {
		case msg := <-ch:
			msgs = append(msgs, msg...)
		case <-deadline:
			t.Fatalf("timed out waiting for %d messages, got %d", count, len(msgs))
		}
	}
	return msgs
}

func drainMessages(ch <-chan []Message) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func TestClientStartAddToBuffer(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)

	assert.Equal(t, c.ring.Size(), 0)
	ch <- Message{}
	time.Sleep(1 * time.Millisecond)
	assert.Equal(t, c.ring.Size(), 1)
}

func TestClientStartAddToBufferOverSize(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 100)

	assert.Equal(t, c.ring.Size(), 0)
	for i := 0; i <= 1000; i++ {
		ch <- Message{Id: strconv.Itoa(i)}
	}
	assert.Equal(t, c.ring.Size(), 100)
	time.Sleep(1 * time.Millisecond)

	msg, err := c.ring.PeekIdx(0)
	assert.Equal(t, err, nil)
	assert.Equal(t, msg.Id, "901")

	msg, err = c.ring.PeekIdx(99)
	assert.Equal(t, err, nil)
	assert.Equal(t, msg.Id, "1000")
}

func TestClientJoinSingle(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(10, true)

	ch <- Message{Content: "foo"}

	msg := <-client.ch

	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "foo", msg[0].Content)
}

func TestClientJoinSingleAfterMessage(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	ch <- Message{Content: "foo"}
	client := c.Join(10, true)
	msg := <-client.ch

	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "foo", msg[0].Content)
}

func TestClientJoinSingleTailLen(t *testing.T) {
	// tailLen is shorter than num of messages produced

	ch := make(chan Message)
	c := NewClients(ch, 1000)

	for i := 0; i < 20; i++ {
		ch <- Message{Content: strconv.Itoa(i)}
	}
	time.Sleep(1 * time.Millisecond)
	client := c.Join(10, true)

	msg := <-client.ch

	assert.Equal(t, 10, len(msg))
	assert.Equal(t, "10", msg[0].Content)
	assert.Equal(t, "19", msg[len(msg)-1].Content)
}

func TestClientJoinMultiple(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client1 := c.Join(10, true)
	client2 := c.Join(10, true)
	client3 := c.Join(10, true)

	ch <- Message{Content: "foo"}

	msg := <-client1.ch
	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "foo", msg[0].Content)

	msg = <-client2.ch
	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "foo", msg[0].Content)

	msg = <-client3.ch
	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "foo", msg[0].Content)
}

func TestClientBulkWindow(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client1 := c.Join(10, true)

	ch <- Message{Content: "foo1"}
	ch <- Message{Content: "foo2"}
	ch <- Message{Content: "foo3"}
	ts := time.Now()
	messages := <-client1.ch

	assert.Equal(t, 3, len(messages))
	assert.GreaterOrEqual(t, int64(time.Since(ts).Milliseconds()), BULK_WINDOW_MS)
	assert.Equal(t, "foo1", messages[0].Content)
	assert.Equal(t, "foo2", messages[1].Content)
	assert.Equal(t, "foo3", messages[2].Content)
}

func TestClientSignalQuit(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)

	cl := c.Join(10, true)
	c.Close(cl.id)
}

func TestClientCloseError(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)

	c.Close("1")
	c.Close("2")
}

func TestClientStopFollowAndResume(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(0, true)
	closed := false

	i := 0
	go func() {
		for {
			i++
			time.Sleep(1 * time.Millisecond)
			if closed {
				return
			}
			ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
		}
	}()
	time.Sleep(10 * time.Millisecond)

	delivered := 0

	lastMsgContent := ""
	BULK_WINDOW_MS = 1

L:
	for {
		select {
		case msg := <-client.ch:
			delivered += len(msg)
			lastMsgContent = msg[len(msg)-1].Content
			c.PauseFollowing(client.id)
		default:
			//once drained, stop listening
			if delivered > 0 {
				assert.Less(t, i, delivered+5)
				// log.Println("Lasg seen", lastMsgContent)
				break L
			}
		}
	}

	// assert channel is empty bec following is stopped
	assert.Equal(t, len(client.ch), 0)
	time.Sleep(10 * time.Millisecond)

	// after some time, assert channel is empty bec following is stopped
	assert.Equal(t, len(client.ch), 0)

	// resume
	c.ResumeFollowing(client.id, true)
	time.Sleep(10 * time.Millisecond)
	// after some time, assert channel has messages
	assert.Greater(t, len(client.ch), 0)

	msg := <-client.ch
	i1, _ := strconv.Atoi(lastMsgContent)
	i2, _ := strconv.Atoi(msg[0].Content)

	assert.Equal(t, i1+1, i2)

	BULK_WINDOW_MS = 100

	closed = true
	close(ch)
}

func TestClientsStats(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	c.Join(0, true)

	i := 0
	st := time.Now()
	for {
		if i >= 100 {
			break
		}
		i++
		ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
	}
	time.Sleep(1 * time.Millisecond)
	stop := time.Now()
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, c.Stats().Count, 100)
	assert.LessOrEqual(t, st.UnixMicro(), c.Stats().FirstMessageAt.UnixMicro())
	assert.GreaterOrEqual(t, stop.UnixMicro(), c.Stats().LastMessageAt.UnixMicro())
}

func TestClientStats(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(0, true)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	i := 0
	for {
		if i >= 100 {
			break
		}
		i++
		ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
	}

	i2 := 0
	for {
		msg := <-client.ch
		i2 += len(msg)

		if i2 > 80 {
			c.PauseFollowing(client.id)
			break
		}
	}

	for {
		if i >= 200 {
			break
		}
		i++
		ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
	}
	time.Sleep(time.Millisecond)
	assert.Equal(t, c.Stats().Count, 200)

	stats := c.ClientStats(client.id)
	assert.Equal(t, stats.LastDeliveredIdIdx+1, i2) //adding 1 to reflect count instead of index which starts at 0
	assert.Equal(t, stats.CountToTail, 200-i2+1)    // adding 1 to index to reflect count which is returned from stats

}

func TestClientStatsWithLoading(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(0, false)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	i := 0
	for {
		if i >= 100 {
			break
		}
		i++
		ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
	}
	time.Sleep(time.Millisecond)
	c.Load(client.id, 20, 20, true) // we're including the first element

	assert.Equal(t, c.Stats().Count, 100)

	stats := c.ClientStats(client.id)
	assert.Equal(t, stats.LastDeliveredIdIdx, 38) //started at idx 19 inclusive, plus 19 elems is 38
	assert.Equal(t, stats.CountToTail, 62)        // adding 1 to index to reflect count which is returned from stats

}

func TestClientPeekLog(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	c.Join(0, true)

	i := 0
	for {
		if i > 100 {
			break
		}
		i++
		ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
	}

	msgs := c.PeekLog([]int{2, 5, 8, 9999999999}) // 99999 will not be found

	assert.Equal(t, len(msgs), 3)
	assert.Equal(t, msgs[0].Id, "3")
	assert.Equal(t, msgs[1].Id, "6")
	assert.Equal(t, msgs[2].Id, "9")
}

func TestClientLoad(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(0, true)
	closed := false

	i := 0
	go func() {
		for {
			if closed {
				return
			}
			i++
			time.Sleep(1 * time.Millisecond / 10)
			ch <- Message{Content: strconv.Itoa(i), Id: strconv.Itoa(i)}
		}
	}()
	time.Sleep(10 * time.Millisecond)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
		closed = true
	}()

	c.PauseFollowing(client.id)
	drainMessages(client.ch)

	assert.Equal(t, client.bufferLen(), 0)

	c.Load(client.id, 10, 25, true)
	buffer := readMessagesUntil(t, client.ch, 25)
	assert.Equal(t, len(buffer), 25)
	assert.Equal(t, buffer[0].Id, "10")
	assert.Equal(t, buffer[24].Id, "34")

	c.Load(client.id, 100, 25, false)
	buffer = readMessagesUntil(t, client.ch, 25)
	assert.Equal(t, len(buffer), 25)
	assert.Equal(t, buffer[0].Id, "101")
	assert.Equal(t, buffer[24].Id, "125")

}

func TestClientJoinMultipleConcurrent(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	const clientCount = 20
	clients := make([]*Client, clientCount)
	wg := sync.WaitGroup{}
	wg.Add(clientCount)

	for i := 0; i < clientCount; i++ {
		go func(idx int) {
			defer wg.Done()
			clients[idx] = c.Join(10, true)
		}(i)
	}
	wg.Wait()

	ch <- Message{Id: "1", Content: "fanout"}

	for _, cl := range clients {
		msg := readMessages(t, cl.ch)
		assert.Equal(t, 1, len(msg))
		assert.Equal(t, "fanout", msg[0].Content)
	}
}

func TestClientPauseDoesNotStopOtherClients(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client1 := c.Join(0, true)
	client2 := c.Join(0, true)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	c.PauseFollowing(client1.id)

	ch <- Message{Id: "1", Content: "only-client-2"}

	msg := readMessages(t, client2.ch)
	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "only-client-2", msg[0].Content)
	assert.Equal(t, 0, len(client1.ch))
}

func TestClientLoadDoesNotStopOtherClients(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client1 := c.Join(0, true)
	client2 := c.Join(0, true)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	for i := 1; i <= 20; i++ {
		ch <- Message{Id: strconv.Itoa(i), Content: strconv.Itoa(i)}
	}

	readMessagesUntil(t, client1.ch, 20)
	readMessagesUntil(t, client2.ch, 20)

	c.Load(client1.id, 5, 5, true)
	loaded := readMessagesUntil(t, client1.ch, 5)
	assert.Equal(t, "5", loaded[0].Id)
	assert.Equal(t, "9", loaded[4].Id)

	ch <- Message{Id: "21", Content: "tail"}

	msg := readMessages(t, client2.ch)
	assert.Equal(t, 1, len(msg))
	assert.Equal(t, "tail", msg[0].Content)
	assert.Equal(t, 0, len(client1.ch))
}

func TestClientConcurrentJoinCloseAndSend(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)

	BULK_WINDOW_MS = 1
	defer func() {
		BULK_WINDOW_MS = 100
	}()

	wg := sync.WaitGroup{}

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cl := c.Join(5, true)
			if idx%2 == 0 {
				c.PauseFollowing(cl.id)
			}
			c.Close(cl.id)
			c.Close(cl.id)
		}(i)
	}

	for i := 0; i < 100; i++ {
		ch <- Message{Id: strconv.Itoa(i), Content: strconv.Itoa(i)}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for concurrent clients")
	}
}

func TestClientCloseIsIdempotent(t *testing.T) {
	ch := make(chan Message)
	c := NewClients(ch, 1000)
	client := c.Join(0, true)

	c.Close(client.id)
	c.Close(client.id)
	c.Close("missing-client")
}
