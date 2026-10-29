package kook

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// mockGateway 模拟 Kook 网关：可控制断开与重连次数
type mockGateway struct {
	mu          sync.Mutex
	sessions    int  // 完成握手的连接数
	dropFirst   bool // 第一条连接在收到一次 ping 后立即断开
	pingSeen    int  // 总共收到的 ping 数
	gatewayURL  string
	apiServer   *httptest.Server
	wsUpgrader  websocket.Upgrader
	lastPingGap time.Duration // 最近两次 ping 的间隔（用于检测旧协程是否还在发 ping）
	lastPingAt  time.Time
	conns       []*websocket.Conn
}

func (m *mockGateway) handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := m.wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	hello, _ := json.Marshal(Event{Signal: EventSignalHello, Data: json.RawMessage(`{"code":0,"session_id":"s1"}`)})
	if err := conn.WriteMessage(websocket.TextMessage, hello); err != nil {
		return
	}

	m.mu.Lock()
	m.sessions++
	isFirst := m.sessions == 1
	m.conns = append(m.conns, conn)
	m.mu.Unlock()

	for {
		mt, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		var p Event
		if err := json.Unmarshal(msg, &p); err != nil || mt != websocket.TextMessage {
			continue
		}
		if p.Signal == EventSignalPing {
			m.mu.Lock()
			now := time.Now()
			if !m.lastPingAt.IsZero() {
				m.lastPingGap = now.Sub(m.lastPingAt)
			}
			m.lastPingAt = now
			m.pingSeen++
			seen := m.pingSeen
			m.mu.Unlock()

			pong, _ := json.Marshal(Event{Signal: EventSignalPong})
			_ = conn.WriteMessage(websocket.TextMessage, pong)

			// 第一条连接在首次 ping 后即踢掉客户端，模拟服务器侧掉线
			if m.dropFirst && isFirst && seen >= 1 {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseGoingAway, ""), time.Now().Add(time.Second))
				_ = conn.Close()
				return
			}
		}
	}
}

func newMockGateway(t *testing.T, dropFirst bool) *mockGateway {
	t.Helper()
	m := &mockGateway{dropFirst: dropFirst, wsUpgrader: websocket.Upgrader{}}
	mux := http.NewServeMux()
	mux.HandleFunc("/gateway/ws", m.handleWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// httptest 地址形如 http://127.0.0.1:port，转成 ws://
	m.gatewayURL = "ws" + strings.TrimPrefix(srv.URL, "http") + "/gateway/ws"
	m.apiServer = srv
	return m
}

// TestReconnectAfterServerDrop 验证：服务器断开后 SDK 能自动重连，
// 且重连后心跳/监听协程只作用于新连接（不再被旧协程误杀）。
func TestReconnectAfterServerDrop(t *testing.T) {
	m := newMockGateway(t, true)

	s := New("test-token", &nopLogger{})
	// 直接指定 gateway，绕过 REST 获取
	s.gateway = m.gatewayURL

	if err := s.Open(); err != nil {
		t.Fatalf("open failed: %v", err)
	}

	// 等待第一代连接被服务器踢掉并完成重连（心跳周期 30s，留足余量）
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		sessions := m.sessions
		m.mu.Unlock()
		if sessions >= 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	m.mu.Lock()
	sessions := m.sessions
	m.mu.Unlock()
	if sessions < 2 {
		t.Fatalf("expected reconnect after server drop, got %d sessions", sessions)
	}

	// 连接必须保持存活：观察到 >=2 次 ping（第一代 1 次 + 新连接至少 1 次）
	deadline = time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		m.mu.Lock()
		pings := m.pingSeen
		m.mu.Unlock()
		if pings >= 3 {
			s.Close()
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	m.mu.Lock()
	pings := m.pingSeen
	m.mu.Unlock()
	s.Close()
	t.Fatalf("connection did not stay alive after reconnect: only %d pings seen", pings)
}

// TestHeartbeatGoroutineExitsAfterSupersede 验证旧 heartbeat 协程在连接被替换后退出，
// 不会向已关闭的旧连接继续写 ping（体现为旧连接上的写错误不会触发多余的 reconnect 循环）。
func TestHeartbeatGoroutineExitsAfterSupersede(t *testing.T) {
	m := newMockGateway(t, false)

	s := New("test-token", &nopLogger{})
	s.gateway = m.gatewayURL

	if err := s.Open(); err != nil {
		t.Fatalf("open failed: %v", err)
	}
	s.RLock()
	gen1 := s.connGen
	conn1 := s.wsConn
	s.RUnlock()

	// 主动关闭再重开，制造新旧两代
	if err := s.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if err := s.Open(); err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	s.RLock()
	gen2 := s.connGen
	conn2 := s.wsConn
	s.RUnlock()

	if gen1 == gen2 {
		t.Fatalf("expected generation bump, got %d both times", gen1)
	}
	if conn1 == conn2 {
		t.Fatal("expected a new connection object")
	}

	// 旧协程应当退出；等一个心跳周期以上，新连接仍在收 ping
	time.Sleep(35 * time.Second)
	m.mu.Lock()
	pings := m.pingSeen
	m.mu.Unlock()
	if pings == 0 {
		t.Fatal("no pings received on the new connection; heartbeat goroutine appears dead")
	}

	s.RLock()
	stillSame := s.wsConn == conn2 && s.connGen == gen2
	s.RUnlock()
	if !stillSame {
		t.Fatal("new connection was killed unexpectedly (likely by a stale goroutine)")
	}
	s.Close()
}

type nopLogger struct{}

func (nopLogger) Trace() Entry { return nopEntry{} }
func (nopLogger) Debug() Entry { return nopEntry{} }
func (nopLogger) Info() Entry  { return nopEntry{} }
func (nopLogger) Warn() Entry  { return nopEntry{} }
func (nopLogger) Error() Entry { return nopEntry{} }
func (nopLogger) Fatal() Entry { return nopEntry{} }

type nopEntry struct{}

func (nopEntry) Caller(depth int) Entry                    { return nopEntry{} }
func (nopEntry) Str(key, val string) Entry                 { return nopEntry{} }
func (nopEntry) Strs(key string, s []string) Entry         { return nopEntry{} }
func (nopEntry) Int(key string, n int) Entry               { return nopEntry{} }
func (nopEntry) Int64(key string, n int64) Entry           { return nopEntry{} }
func (nopEntry) Err(key string, err error) Entry           { return nopEntry{} }
func (nopEntry) Bool(key string, b bool) Entry             { return nopEntry{} }
func (nopEntry) Dur(key string, d time.Duration) Entry     { return nopEntry{} }
func (nopEntry) Time(key string, t time.Time) Entry        { return nopEntry{} }
func (nopEntry) Float64(key string, f float64) Entry       { return nopEntry{} }
func (nopEntry) Bytes(key string, val []byte) Entry        { return nopEntry{} }
func (nopEntry) IPAddr(key string, ip net.IP) Entry        { return nopEntry{} }
func (nopEntry) Interface(key string, i interface{}) Entry { return nopEntry{} }
func (nopEntry) Msg(msg string)                            {}
func (nopEntry) Msgf(format string, v ...interface{})      {}
