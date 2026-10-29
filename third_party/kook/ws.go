package kook

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ErrWSAlreadyOpen is the error when connecting with connected websocket.
var ErrWSAlreadyOpen = errors.New("websocket is already opened")

// Open starts a websocket connection. It does not block the function.
func (s *Session) Open() (err error) {
	//s.log(LogInfo, "called")
	addCaller(s.Logger.Info()).Msg("called")

	s.Lock()
	defer s.Unlock()

	if s.wsConn != nil {
		addCaller(s.Logger.Error()).Msg("websocket is already open")
		return ErrWSAlreadyOpen
	}

	if s.gateway == "" {
		s.gateway, err = s.Gateway()
		if err != nil {
			addCaller(s.Logger.Error()).Err("err", err).Msg("unable to get ws gateway")
			return
		}
	}

	//s.log(LogInfo, "connecting to gateway %s", s.gateway)
	addCaller(s.Logger.Info()).Str("gateway_url", s.gateway).Msg("connecting to gateway")
	s.wsConn, _, err = websocket.DefaultDialer.Dial(s.gateway, http.Header{})
	if err != nil {
		addCaller(s.Logger.Error()).
			Str("gateway_url", s.gateway).
			Err("err", err).
			Msg("error connecting to gateway")
		//s.log(LogError, "error connecting to gateway %s, %s", s.gateway, err.Error())
		s.gateway = ""
		s.wsConn = nil
		return
	}
	s.wsConn.SetCloseHandler(func(code int, text string) error {
		return nil
	})

	// 握手读取期间临时释放写锁，避免服务器异常时 ReadMessage 永久阻塞
	// 并持有锁，导致 Close/重启全部卡死。
	handshakeConn := s.wsConn
	s.Unlock()
	mt, m, err := handshakeConn.ReadMessage()
	s.Lock()
	if s.wsConn != handshakeConn {
		// 期间连接被关闭或替换，放弃本次握手
		_ = handshakeConn.Close()
		if err == nil {
			err = errors.New("connection replaced during handshake")
		}
		return
	}
	if err != nil {
		addCaller(s.Logger.Error()).Err("err", err).Msg("error reading message from websocket")
		_ = s.wsConn.Close()
		s.wsConn = nil
		return
	}
	e, err := s.onEvent(mt, m)
	if err != nil {
		addCaller(s.Logger.Error()).Err("err", err).Msg("error parsing event")
		_ = s.wsConn.Close()
		s.wsConn = nil
		return
	}
	if e.Signal != EventSignalHello {
		s.gateway = ""
		_ = s.wsConn.Close()
		s.wsConn = nil
		err = fmt.Errorf("expecting signal hello, got singal %d", e.Signal)
		return err
	}
	//s.log(LogInfo, "signal hello received")
	addCaller(s.Logger.Info()).Msg("signal hello received")
	s.LastHeartbeatAck = time.Now().UTC()

	var h EventDataHello
	if err = json.Unmarshal(e.Data, &h); err != nil {
		addCaller(s.Logger.Error()).Err("err", err).Msg("error unmarshalling hello")
		err = fmt.Errorf("error unmarshalling hello, %s", err.Error())
		_ = s.wsConn.Close()
		s.wsConn = nil
		return
	}
	if h.Code != EventStatusOk {
		s.gateway = ""
		addCaller(s.Logger.Error()).Int("code", int(h.Code)).Msg("error status is not ok")
		_ = s.wsConn.Close()
		s.wsConn = nil
		err = fmt.Errorf("expecting status ok, received %d", h.Code)
		return
	}

	// 每代连接使用独立的 done 通道和世代号：旧代协程据此退出，
	// 不会误关/误杀重连后的新连接。
	s.connGen++
	s.listening = make(chan interface{})
	go s.heartbeat(s.connGen, s.wsConn, s.listening)
	go s.listen(s.connGen, s.wsConn, s.listening)

	addCaller(s.Logger.Info()).Msg("exiting")

	//s.log(LogInfo, "exiting")

	return
}

func (s *Session) onEvent(messageType int, message []byte) (e *Event, err error) {
	var reader io.Reader
	reader = bytes.NewBuffer(message)

	if messageType == websocket.BinaryMessage {
		z, err2 := zlib.NewReader(reader)
		if err2 != nil {
			addCaller(s.Logger.Error()).Err("err", err).Msg("error decompressing websocket message")
			//s.log(LogError, "error uncompressing websocket message, %s", err)
			return nil, err2
		}
		defer func() {
			err3 := z.Close()
			if err3 != nil {
				addCaller(s.Logger.Warn()).Err("err", err3).Msg("error closing zlib")
				//s.log(LogWarning, "error closing zlib, %s", err)
			}
		}()

		reader = z
	}

	jsonDecoder := json.NewDecoder(reader)
	if err = jsonDecoder.Decode(&e); err != nil {
		addCaller(s.Logger.Error()).Err("err", err).Msg("error decoding websocket message")
		//s.log(LogError, "error decoding websocket message, %s", err)
		return
	}

	addCaller(s.Logger.Debug()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("received event")
	//s.log(LogDebug, "Signal: %d, Sequence: %d, Data: %s", e.Signal, e.SequenceNumber, string(e.Data))

	if e.Signal == EventSignalHello {
		return
	}

	if e.Signal == EventSignalPong {
		s.Lock()
		s.LastHeartbeatAck = time.Now().UTC()
		s.Unlock()
		addCaller(s.Logger.Debug()).Msg("got heartbeat ACK")
		//s.log(LogDebug, "got heartbeat ACK")
		return
	}

	if e.Signal == EventSignalReconnect {
		addCaller(s.Logger.Info()).Msg("closing current ws and reconnecting in response to Reconnect signal")
		//s.log(LogInfo, "closing current ws and reconnecting in response to Reconnect signal")
		s.CloseWithCode(websocket.CloseServiceRestart)
		s.Lock()
		s.gateway = ""
		atomic.StoreInt64(s.sequence, 0)
		s.snStore.Clear()
		s.Unlock()
		s.reconnect()
		return
	}

	if e.Signal == EventSignalResumeAck {
		addCaller(s.Logger.Info()).Msg("all missing message are sent, received Resume Ack signal")
		//s.log(LogInfo, "all missing message are sent, received Resume Ack signal")
		return
	}

	if e.Signal != EventSignalEvent {
		addCaller(s.Logger.Error()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown signal")
		//s.log(LogError, "unknown signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		return
	}

	atomic.AddInt64(s.sequence, 1)
	var exist bool
	func() {
		s.snStore.Lock()
		defer s.snStore.Unlock()
		exist = s.snStore.TestAndInsert(e.SequenceNumber)
	}()
	if exist && e.SequenceNumber != 0 {
		return nil, nil
	}
	data := EventData{}

	if err = json.Unmarshal(e.Data, &data); err != nil {
		addCaller(s.Logger.Error()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal event data error")

		//s.log(LogError, "unmarshal event data error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
		return
	}
	if data.Type == MessageTypeSystem {
		if data.ChannelType == "WEBHOOK_CHALLENGE" {
			return e, errWebhookVerify
		}
		sys := EventDataSystem{}
		if err = json.Unmarshal(data.Extra, &sys); err != nil {
			addCaller(s.Logger.Error()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal system event extra.body error")
			//s.log(LogError, "unmarshal system event extra.body error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			return
		}
		if eh, ok := registeredEventHandler[sys.Type]; ok {
			t := eh.New()
			ex := t.GetExtra()
			if err = json.Unmarshal(sys.Body, ex); err != nil {
				addCaller(s.Logger.Error()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal extra error")
				//s.log(LogError, "unmarshal extra error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			}
			s.handleEvent(eh.Type(), data.EventDataGeneral, t)
		} else {
			addCaller(s.Logger.Warn()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown system message event")
			//s.log(LogWarning, "unknown system message event: signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		}
	} else {
		if eh, ok := registeredEventHandler[strconv.Itoa(int(data.Type))]; ok {
			t := eh.New()
			ex := t.GetExtra()
			if err = json.Unmarshal(data.Extra, ex); err != nil {
				addCaller(s.Logger.Error()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Err("err", err).Msg("unmarshal extra error")

				//s.log(LogError, "unmarshal extra error: %s\nsignal: %d, seq: %d, data: %s", err, e.Signal, e.SequenceNumber, string(e.Data))
			}
			s.handleEvent(eh.Type(), data.EventDataGeneral, t)
		} else {
			addCaller(s.Logger.Warn()).Int("signal", int(e.Signal)).Int64("seq", e.SequenceNumber).Bytes("data", e.Data).Msg("unknown message event")
			//s.log(LogWarning, "unknown system message event: signal: %d, seq: %d, data: %s", e.Signal, e.SequenceNumber, string(e.Data))
		}
	}

	return nil, err
}

func (s *Session) listen(gen uint64, wsConn *websocket.Conn, listening <-chan interface{}) {
	//s.log(LogInfo, "called")
	addCaller(s.Logger.Info()).Msg("called")

	for {
		messageType, message, err := wsConn.ReadMessage()
		if err != nil {
			// 只有当前世代的连接才允许触发重连，旧代协程直接退出，
			// 防止重连后旧 listen/heartbeat 误杀新连接。
			s.RLock()
			sameConnection := s.wsConn == wsConn && s.connGen == gen
			s.RUnlock()

			if sameConnection {
				addCaller(s.Logger.Warn()).Str("gateway_url", s.gateway).Err("err", err).Msg("error reading from gateway")
				//s.log(LogWarning, "error reading from gateway %s websocket, %s", s.gateway, err)
				err := s.Close()
				if err != nil {
					addCaller(s.Logger.Warn()).Err("err", err).Msg("error closing session connection")
					//s.log(LogWarning, "error closing session connection, %s", err)
				}
				addCaller(s.Logger.Info()).Msg("calling reconnect")
				//s.log(LogInfo, "calling reconnect now.")
				s.reconnect()
			}
			return
		}
		select {
		case <-listening:
			return
		default:
			s.onEvent(messageType, message)
		}
	}
}

type pingSignal struct {
	Signal         EventSignal `json:"s"`
	SequenceNumber int64       `json:"sn"`
}

func (s *Session) heartbeat(gen uint64, wsConn *websocket.Conn, listening <-chan interface{}) {
	addCaller(s.Logger.Info()).Msg("called")
	//s.log(LogInfo, "called")
	if listening == nil || wsConn == nil {
		return
	}
	var err error
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		s.RLock()
		last := s.LastHeartbeatAck
		currentConn := s.wsConn
		currentGen := s.connGen
		s.RUnlock()
		if currentConn != wsConn || currentGen != gen {
			// 连接已被替换，本协程是旧世代，退出
			addCaller(s.Logger.Info()).Int64("gen", int64(gen)).Msg("heartbeat goroutine for superseded connection exiting")
			return
		}
		sequence := atomic.LoadInt64(s.sequence)
		//s.log(LogDebug, "sending gateway websocket heartbeat seq %d", sequence)
		addCaller(s.Logger.Debug()).Int64("seq", sequence).Msg("sending gateway websocket heartbeat")

		s.wsMutex.Lock()
		s.LastHeartbeatSent = time.Now().UTC()
		err = wsConn.WriteJSON(pingSignal{
			Signal:         EventSignalPing,
			SequenceNumber: sequence,
		})
		s.wsMutex.Unlock()
		if err != nil || time.Now().UTC().Sub(last) > 36*time.Second {
			if err != nil {
				addCaller(s.Logger.Error()).Str("gateway_url", s.gateway).Err("err", err).Msg("error sending heartbeat to gateway")
				//s.log(LogError, "error sending heartbeat to gateway %s, %s", s.gateway, err)
			} else {
				addCaller(s.Logger.Error()).Dur("latency", time.Now().UTC().Sub(last)).Msg("ACK not received, reconnect")
				//s.log(LogError, "haven't gotten a heartbeat ACK in %v, triggering a reconnection", time.Now().UTC().Sub(last))
			}
			// 双重确认仍是本代连接才触发重连；旧协程超时只退出自己
			s.RLock()
			stillCurrent := s.wsConn == wsConn && s.connGen == gen
			s.RUnlock()
			if !stillCurrent {
				addCaller(s.Logger.Info()).Int64("gen", int64(gen)).Msg("connection already superseded, heartbeat goroutine exiting")
				return
			}
			s.Close()
			s.reconnect()
			return
		}
		select {
		case <-ticker.C:
		case <-listening:
			return
		}

	}
}

func (s *Session) reconnect() {
	addCaller(s.Logger.Info()).Msg("called")
	//s.log(LogInfo, "called")
	var err error
	wait := time.Duration(1)
	for {
		//s.log(LogInfo, "trying to reconnect to gateway")
		addCaller(s.Logger.Info()).Msg("trying to reconnect to gateway")
		err = s.Open()
		if err == nil {
			addCaller(s.Logger.Info()).Msg("successfully reconnected to gateway")
			//s.log(LogInfo, "successfully reconnected to gateway")
			return
		}

		if err == ErrWSAlreadyOpen {
			addCaller(s.Logger.Info()).Msg("websocket already exists, no need to reconnect")
			//s.log(LogInfo, "websocket already exists, no need to reconnect")
			return
		}

		addCaller(s.Logger.Error()).Err("err", err).Msg("error reconnecting to gateway")
		//s.log(LogError, "error reconnecting to gateway, %s", err)
		<-time.After(wait * time.Second)
		wait *= 2
		if wait > 600 {
			wait = 600
		}
	}
}

// Close closes a websocket connection.
func (s *Session) Close() (err error) {
	return s.CloseWithCode(websocket.CloseNormalClosure)
}

// CloseWithCode closes a websocket connection with custom websocket closing code.
func (s *Session) CloseWithCode(code int) (err error) {
	addCaller(s.Logger.Info()).Msg("called")
	//s.log(LogInfo, "called")
	s.Lock()
	if s.listening != nil {
		//s.log(LogInfo, "closing listening channel")
		addCaller(s.Logger.Info()).Msg("closing listening channel")
		close(s.listening)
		s.listening = nil
	}
	conn := s.wsConn
	s.wsConn = nil
	s.Unlock()

	if conn != nil {
		//s.log(LogInfo, "sending close frame")
		addCaller(s.Logger.Info()).Msg("sending close frame")
		s.wsMutex.Lock()
		err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, ""))
		s.wsMutex.Unlock()
		if err != nil {
			//s.log(LogInfo, "error closing websocket, %s", err)
			addCaller(s.Logger.Info()).Err("err", err).Msg("error closing websocket")
		}

		// 给对端一点时间接收 close 帧；锁已释放，不会阻塞重连
		time.Sleep(1 * time.Second)
		addCaller(s.Logger.Info()).Msg("closing gateway websocket")
		//s.log(LogInfo, "closing gateway websocket")
		err = conn.Close()
		if err != nil {
			//s.log(LogInfo, "error closing websocket, %s", err)
			addCaller(s.Logger.Info()).Err("err", err).Msg("error closing websocket")
		}
	}
	return
}
