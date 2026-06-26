package http

import (
	"fmt"
	"sync"
	"time"

	"github.com/logdyhq/logdy-core/models"
	"github.com/logdyhq/logdy-core/ring"
	"github.com/logdyhq/logdy-core/utils"

	. "github.com/logdyhq/logdy-core/models"
)

var Ch chan models.Message
var Clients *ClientsStruct

var BULK_WINDOW_MS int64 = 100
var FLUSH_BUFFER_SIZE = 1000

type CursorStatus string

const CURSOR_STOPPED CursorStatus = "stopped"
const CURSOR_FOLLOWING CursorStatus = "following"

type Client struct {
	mu        sync.Mutex
	closeOnce sync.Once
	id        string
	done      chan struct{}
	ch        chan []Message
	buffer    []Message
	closed    bool

	cursorStatus   CursorStatus
	cursorPosition string // last delivered message id
}

func (c *Client) handleMessage(m Message, force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	if !force && c.cursorStatus == CURSOR_STOPPED {
		utils.Logger.Debug("Client: Status stopped discarding message")
		return
	}
	c.buffer = append(c.buffer, m)
}

func (c *Client) flushBuffer() {
	batches := c.drainBuffer()
	for _, batch := range batches {
		select {
		case c.ch <- batch:
		case <-c.done:
			return
		}
	}
}

func (c *Client) drainBuffer() [][]Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}
	if len(c.buffer) == 0 {
		return nil
	}

	c.cursorPosition = c.buffer[len(c.buffer)-1].Id
	batches := [][]Message{}
	for i := 0; i < len(c.buffer); i += FLUSH_BUFFER_SIZE {
		end := i + FLUSH_BUFFER_SIZE
		if end > len(c.buffer) {
			end = len(c.buffer)
		}

		batch := make([]Message, end-i)
		copy(batch, c.buffer[i:end])
		batches = append(batches, batch)
	}

	c.clearBuffer()
	return batches
}

func (c *Client) clearBuffer() {
	c.buffer = []Message{}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	})
}

func (c *Client) waitForBufferDrain() {
	for {
		c.mu.Lock()
		bufferLen := len(c.buffer)
		closed := c.closed
		c.mu.Unlock()

		if bufferLen == 0 || closed {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (c *Client) setCursorStatus(status CursorStatus) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	c.cursorStatus = status
}

func (c *Client) getCursorStatus() CursorStatus {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cursorStatus
}

func (c *Client) getCursorPosition() string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.cursorPosition
}

func (c *Client) appendMessages(messages []Message, force bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}
	for _, msg := range messages {
		if !force && c.cursorStatus == CURSOR_STOPPED {
			continue
		}
		c.buffer = append(c.buffer, msg)
	}
}

func (c *Client) bufferLen() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.buffer)
}

func (c *Client) bufferSnapshot() []Message {
	c.mu.Lock()
	defer c.mu.Unlock()

	buffer := make([]Message, len(c.buffer))
	copy(buffer, c.buffer)
	return buffer
}

// Messages are delivered in bulks to avoid
// ddossing the client (browser) with too many messages produced
// in a very short timespan
func (c *Client) startBufferFlushLoop() {
	for {
		select {
		case <-c.done:
			utils.Logger.Debug("Client: received done signal, quitting")
			return
		case <-time.After(time.Millisecond * time.Duration(BULK_WINDOW_MS)):
			bufferLen := c.bufferLen()
			if bufferLen == 0 {
				continue
			}

			utils.Logger.WithField("count", bufferLen).Debug("Client: Flushing buffer")
			c.flushBuffer()
		}

	}
}

func NewClient() *Client {
	c := &Client{
		done:           make(chan struct{}),
		ch:             make(chan []Message, BULK_WINDOW_MS*25),
		cursorStatus:   CURSOR_STOPPED,
		cursorPosition: "",
		id:             utils.RandStringRunes(6),
	}

	go c.startBufferFlushLoop()

	return c
}

type ClientsStruct struct {
	started            bool
	startMu            sync.Mutex
	registryMu         sync.RWMutex
	bufferMu           sync.RWMutex
	mainChan           <-chan Message
	clients            map[string]*Client
	ring               *ring.RingQueue[Message]
	currentlyConnected int
	stats              Stats
}

func NewClients(msgs <-chan Message, maxCount int64) *ClientsStruct {
	if maxCount == 0 {
		maxCount = 100_000
	}

	cls := &ClientsStruct{
		mainChan:           msgs,
		clients:            map[string]*Client{},
		currentlyConnected: 0,
		ring:               ring.NewRingQueue[Message](maxCount),
		stats: Stats{
			MaxCount: maxCount,
			Count:    0,
		},
	}

	go cls.Start()

	return cls
}

func (c *ClientsStruct) GetClient(clientId string) (*Client, bool) {
	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	cl, ok := c.clients[clientId]
	return cl, ok
}

func (c *ClientsStruct) Load(clientId string, startCount int, count int, includeStart bool) {
	cl, exists := c.GetClient(clientId)
	if !exists {
		return
	}

	c.PauseFollowing(clientId)
	cl.waitForBufferDrain()

	seen := false
	sent := 0
	msgs := []Message{}

	c.bufferMu.RLock()
	c.ring.Scan(func(msg Message, i int) bool {
		if i+1 == startCount {
			seen = true
			if !includeStart {
				return false
			}
		}

		if !seen {
			return false
		}

		sent++
		msgs = append(msgs, msg)

		if count > 0 && sent >= count {
			return true
		}
		return false
	})
	c.bufferMu.RUnlock()

	cl.appendMessages(msgs, true)
	cl.flushBuffer()

}

func (c *ClientsStruct) PeekLog(idxs []int) []Message {
	msgs := []Message{}

	c.bufferMu.RLock()
	defer c.bufferMu.RUnlock()

	for _, idx := range idxs {
		if idx < 0 {
			continue
		}
		if c.ring.Size()-1 < idx {
			continue
		}
		msg, err := c.ring.PeekIdx(idx)
		if err != nil {
			panic(err)
		}
		msgs = append(msgs, msg)
	}

	return msgs
}

func (c *ClientsStruct) Stats() Stats {
	c.bufferMu.RLock()
	defer c.bufferMu.RUnlock()

	return c.stats
}
func (c *ClientsStruct) ClientStats(clientId string) ClientStats {
	stats := ClientStats{}
	cl, exists := c.GetClient(clientId)
	if !exists {
		return stats
	}

	stats.LastDeliveredId = cl.getCursorPosition()

	c.bufferMu.RLock()
	c.ring.Scan(func(m Message, idx int) bool {
		if m.Id == stats.LastDeliveredId {
			stats.LastDeliveredIdIdx = idx
			return true
		}

		return false
	})

	stats.CountToTail = c.stats.Count - stats.LastDeliveredIdIdx
	c.bufferMu.RUnlock()

	return stats
}

func (c *ClientsStruct) ResumeFollowing(clientId string, sinceCursor bool) {
	//pump back the items until last element seen
	cl, exists := c.GetClient(clientId)
	if !exists {
		return
	}

	msgs := []Message{}
	if sinceCursor {
		seen := false
		cursorPosition := cl.getCursorPosition()

		c.bufferMu.RLock()
		c.ring.Scan(func(msg Message, _ int) bool {
			if msg.Id == cursorPosition {
				seen = true
				return false
			}

			if !seen {
				return false
			}

			msgs = append(msgs, msg)
			return false
		})
		c.bufferMu.RUnlock()

	}
	cl.appendMessages(msgs, true)
	cl.flushBuffer()
	cl.setCursorStatus(CURSOR_FOLLOWING)
}

func (c *ClientsStruct) PauseFollowing(clientId string) {
	cl, exists := c.GetClient(clientId)
	if !exists {
		return
	}

	cl.setCursorStatus(CURSOR_STOPPED)
	cl.waitForBufferDrain()
}

// starts a delivery channel to all clients
func (c *ClientsStruct) Start() {
	c.startMu.Lock()
	if c.started {
		c.startMu.Unlock()
		utils.Logger.Debug("Clients delivery loop already started")
		return
	}
	c.started = true
	c.startMu.Unlock()

	for msg := range c.mainChan {
		c.bufferMu.Lock()
		if c.stats.FirstMessageAt.IsZero() {
			c.stats.FirstMessageAt = time.Now()
		}

		c.ring.PushSafe(msg)
		if c.stats.Count < int(c.stats.MaxCount) {
			c.stats.Count++
		}

		c.stats.LastMessageAt = time.Now()
		c.bufferMu.Unlock()

		for _, ch := range c.clientSnapshot() {
			ch.handleMessage(msg, false)
		}
	}
}

func (c *ClientsStruct) Join(tailLen int, shouldFollow bool) *Client {
	cl := NewClient()

	c.bufferMu.RLock()
	// deliver last N messages from a buffer upon connection
	idx := 0
	if c.ring.Size() > tailLen {
		idx = c.ring.Size() - tailLen
	}
	sl, err := c.ring.PeekSlice(idx)

	if err != nil {
		panic(err)
	}

	for _, msg := range sl {
		cl.handleMessage(msg, true)
	}

	if shouldFollow {
		cl.setCursorStatus(CURSOR_FOLLOWING)
	}

	c.registryMu.Lock()
	c.clients[cl.id] = cl
	c.currentlyConnected++
	c.registryMu.Unlock()
	c.bufferMu.RUnlock()

	return cl
}

func (c *ClientsStruct) Close(id string) {
	c.registryMu.Lock()
	cl, ok := c.clients[id]
	if ok {
		delete(c.clients, id)
		c.currentlyConnected--
	}
	c.registryMu.Unlock()

	if !ok {
		return
	}

	cl.close()
}

func (c *ClientsStruct) clientSnapshot() []*Client {
	c.registryMu.RLock()
	defer c.registryMu.RUnlock()

	clients := make([]*Client, 0, len(c.clients))
	for _, cl := range c.clients {
		clients = append(clients, cl)
	}
	return clients
}

func InitChannel() {
	if Ch != nil {
		return
	}

	Ch = make(chan models.Message, 1000)
}

func InitializeClients(config Config) *ClientsStruct {
	if Clients != nil {
		return Clients
	}

	bts := int64(0)

	if config.AppendToFileRotateMaxSize != "" {
		var err error
		bts, err = utils.ParseRotateSize(config.AppendToFileRotateMaxSize)

		if err != nil {
			panic(fmt.Errorf("file rotate size parse error: %w", err))
		}
	}

	mainChan := utils.ProcessIncomingMessagesWithRotation(Ch, config.AppendToFile, config.AppendToFileRaw, bts, 1000)
	Clients = NewClients(mainChan, config.MaxMessageCount)

	return Clients
}
