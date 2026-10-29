package dice

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/zap"

	"Scardice-core/dice/imsdk/onebot/schema"
	emitterTypes "Scardice-core/dice/imsdk/onebot/types"
)

// onebotMusicTestEmitter 捕获 SendGrMsg 的消息链
type onebotMusicTestEmitter struct {
	onebotTestEmitter
	captured schema.MessageChain
}

func (m *onebotMusicTestEmitter) SendGrMsg(_ context.Context, _ int64, msg schema.MessageChain) (*emitterTypes.SendMsgRes, error) {
	m.captured = msg
	return &emitterTypes.SendMsgRes{MessageId: 12345}, nil
}

func newMusicTestAdapter(t *testing.T) (*PlatformAdapterOnebot, *onebotMusicTestEmitter) {
	t.Helper()
	pa := &PlatformAdapterOnebot{logger: zap.NewNop().Sugar()}
	mock := &onebotMusicTestEmitter{}
	pa.sendEmitter = mock
	return pa, mock
}

// TestOnebotMusicCQCodePassThrough 点歌类 CQ 码应作为 music 消息段原样发到协议端。
// 回归背景：升级核心后 [CQ:music] 曾变成 JSON 编码字符串发出去，
// QQ 客户端渲染为"发送者版本过低，无法展示内容"。
// 卡片合成/签名是协议端（SnowLuma musicSignUrl 等）的事，核心保持标准透传。
func TestOnebotMusicCQCodePassThrough(t *testing.T) {
	pa, mock := newMusicTestAdapter(t)

	// 典型点歌插件的输出形态
	raw := `[CQ:music,type=custom,url=https://music.example.com/song/1,audio=https://music.example.com/audio/1,title=测试歌曲,content=测试歌手,image=https://music.example.com/cover/1.jpg]`

	pa.SendToGroup(nil, "QQ-Group:12345", raw, "")

	if len(mock.captured) == 0 {
		t.Fatal("no message captured")
	}
	marshaled, err := json.Marshal(mock.captured)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("captured chain: %s", marshaled)

	// 消息链应包含一个 type=music 的段，其 data 是对象（含 url/title 等字段）
	for _, seg := range mock.captured {
		if seg.Type != "music" {
			continue
		}
		var data map[string]interface{}
		if err := json.Unmarshal(seg.Data, &data); err != nil {
			t.Fatalf("music segment data is not a JSON object: %s\nfull chain: %s", seg.Data, marshaled)
		}
		if data["type"] != "custom" {
			t.Fatalf("music segment lost type=custom: %s", seg.Data)
		}
		if data["title"] != "测试歌曲" {
			t.Fatalf("music segment lost title: %s", seg.Data)
		}
		return
	}
	t.Fatalf("no music segment in chain: %s", marshaled)
}

// TestOnebotJsonXmlCQCodePassThrough json/xml 卡片段按 OneBot 11 标准把载荷放在
// "data" 字段。协议端（如 SnowLuma 的 json codec）按标准读取该键；核心不做任何
// 键名改写或额外注入。
func TestOnebotJsonXmlCQCodePassThrough(t *testing.T) {
	pa, mock := newMusicTestAdapter(t)

	// data 参数值内的逗号需按 CQ 规范转义为 &#44;
	const payload = `{"app":"com.tencent.music","desc":"点歌"}`
	raw := `前缀 [CQ:json,data={"app":"com.tencent.music"&#44;"desc":"点歌"}] 后缀`
	pa.SendToGroup(nil, "QQ-Group:12345", raw, "")

	if len(mock.captured) == 0 {
		t.Fatal("no message captured")
	}
	marshaled, _ := json.Marshal(mock.captured)
	t.Logf("captured chain: %s", marshaled)

	var sawJSON bool
	for _, seg := range mock.captured {
		if seg.Type != "json" {
			continue
		}
		sawJSON = true
		var data map[string]interface{}
		if err := json.Unmarshal(seg.Data, &data); err != nil {
			t.Fatalf("json segment data is not a JSON object: %s\nfull chain: %s", seg.Data, marshaled)
		}
		if data["data"] != payload {
			t.Fatalf("json segment lost standard data payload: %s", seg.Data)
		}
		if _, exists := data["text"]; exists {
			t.Fatalf("json segment unexpectedly gained a text field: %s", seg.Data)
		}
	}
	if !sawJSON {
		t.Fatalf("no json segment in chain: %s", marshaled)
	}

	// CQ 码后的文本不应被截断
	var sawTrailingText bool
	for _, seg := range mock.captured {
		if seg.Type != "text" {
			continue
		}
		var data map[string]interface{}
		if err := json.Unmarshal(seg.Data, &data); err == nil && data["text"] == " 后缀" {
			sawTrailingText = true
		}
	}
	if !sawTrailingText {
		t.Fatalf("text after the CQ code was dropped: %s", marshaled)
	}
}
