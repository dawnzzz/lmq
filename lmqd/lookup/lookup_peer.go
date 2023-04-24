package lookup

import (
	"encoding/json"
	"errors"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/internel/protocol"
	"github.com/dawnzzz/lmq/logger"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	maxTry        = 3
	retryInterval = 100 * time.Millisecond

	defaultChanSize = 64
	defaultTimeout  = 3 * time.Second
)

var (
	pingBytes = []byte("PING")
	handler   = &channelsRecvHandler{
		BaseHandler: hamble.BaseHandler{},
	}
)

type lookupPeer struct {
	host         string              // lmq lookup的地址
	port         int                 // lmq lookup的端口
	client       serveriface.IClient // 记录与lmq lookup连接的客户端
	isConnecting atomic.Bool
	cond         *sync.Cond    // 唤醒因为没有连接lookup被阻塞的协程
	interval     time.Duration // 向lmq lookup发送心跳的时间间隔

	channelsRequestChan  chan *channelsReq // sendTopicChannels的响应信息
	channelsResponseChan chan *channelsReq // sendTopicChannels的返回信息
	connectChan          chan struct{}     // 当这个chan中有一个消息时，表示需要与lookup进行连接了
	isClosing            atomic.Bool
	exitChan             chan struct{}
}

type channelsReq struct {
	topicName string
	sendAt    time.Time // 请求发送时间
	data      []string  // 返回的数据
}

type channelsRecvHandler struct {
	hamble.BaseHandler
	peer *lookupPeer
}

func (h *channelsRecvHandler) Handle(request serveriface.IRequest) {
	// 收到了channels消息
	channelsRequestChan := h.peer.channelsRequestChan
	channelsResponseCHan := h.peer.channelsResponseChan
	select {
	case req := <-channelsRequestChan:
		// 反序列化消息
		resp := protocol.ResponseBody{}
		data := request.GetData()
		err := json.Unmarshal(data, &resp)
		if err != nil {
			channelsResponseCHan <- req
			return
		}

		req.data, _ = resp.Data.([]string)
		channelsResponseCHan <- req
	case <-h.peer.exitChan:
	default:
	}
}

func newLookupPeer(lookupAddress string) (*lookupPeer, error) {
	host, portStr, err := net.SplitHostPort(lookupAddress)
	if err != nil {
		return nil, err
	}
	port, _ := strconv.Atoi(portStr)

	peer := &lookupPeer{
		host:     host,
		port:     port,
		interval: config.GlobalLmqdConfig.HeartBeatInterval,

		cond:                 sync.NewCond(&sync.Mutex{}),
		channelsRequestChan:  make(chan *channelsReq, defaultChanSize),
		channelsResponseChan: make(chan *channelsReq, defaultChanSize),
		connectChan:          make(chan struct{}, 1),
		exitChan:             make(chan struct{}),
	}
	return peer, nil
}

func (peer *lookupPeer) start() {
	err := peer.connect() // 需要连接
	if err == nil {
		_ = peer.sendHeartbeatPing() // 建立连接成功，发送一个心跳
	}
	go peer.loop()
}

// 负责定期发送心跳，以及重连
func (peer *lookupPeer) loop() {
	ticker := time.NewTicker(peer.interval)
	for {
		if peer.isClosing.Load() { // 提前结束
			goto exit
		}

		select {
		case <-peer.connectChan:
			// 连接lmq lookup
			err := peer.connect()
			if err != nil {
				continue
			}
			// 建立连接成功，立即发送一个心跳
			ticker.Reset(peer.interval) // 重连之后重置计时器
			_ = peer.sendHeartbeatPing()
		case <-peer.exitChan:
			// 停止
			goto exit
		case <-ticker.C:
			// 发送心跳消息
			_ = peer.sendHeartbeatPing()
		}
	}

exit:
	ticker.Stop() // 关闭心跳计时器
}

// 关闭
func (peer *lookupPeer) close() {
	if !peer.isClosing.CompareAndSwap(false, true) {
		// 已经关闭
		return
	}

	// 关闭exit通道
	close(peer.exitChan)
}

// 连接
func (peer *lookupPeer) connect() (err error) {
	if !peer.isConnecting.CompareAndSwap(false, true) {
		return nil
	}
	defer peer.isConnecting.Store(false)

	if peer.client != nil {
		return nil
	}

	peer.channelsRequestChan = make(chan *channelsReq, defaultChanSize)  // 连接时清空队列
	peer.channelsResponseChan = make(chan *channelsReq, defaultChanSize) // 连接时清空队列

	defer func() {
		if err != nil { // 连接时发生了错误
			if peer.client != nil {
				peer.client.Stop()
				peer.client = nil
			}
		}

		peer.cond.Broadcast() // 唤醒所有阻塞的线程
	}()

	// 与lmq look建立连接
	for i := 0; i < maxTry; i++ { // 每次尝试三次
		peer.client, err = hamble.NewClient("tcp", peer.host, peer.port)
		if err == nil { // 连接成功
			go func() {
				handler.peer = peer
				peer.client.RegisterHandler(protocol.ChannelsID, handler)
				peer.client.Start()
				// 关闭连接进行重连
				select {
				case peer.connectChan <- struct{}{}:
					peer.client.Stop()
					peer.client = nil
				default:
				}
			}()
			break
		}

		time.Sleep(retryInterval)
	}
	if err != nil {
		return
	}

	// 发送identify消息
	address := peer.client.GetConnection().GetConn().LocalAddr().String()
	host, _, _ := net.SplitHostPort(address)
	requestBody := protocol.RequestBody{
		RemoteAddress: peer.client.GetConnection().GetConn().LocalAddr().String(),
		Hostname:      host,
		TcpPort:       config.GlobalLmqdConfig.TcpPort,
	}
	data, err := json.Marshal(requestBody)
	if err != nil {
		return
	}
	_ = peer.client.GetConnection().SendBufMsg(protocol.IdentityID, data)
	if err != nil {
		return
	}

	return nil
}

// 发起心跳消息
func (peer *lookupPeer) sendHeartbeatPing() (err error) {
	peer.cond.L.Lock()
	defer peer.cond.L.Unlock()
	if peer.client == nil {
		select {
		case peer.connectChan <- struct{}{}:
		default:
		}
		return errors.New("lmq lookup server is not connected")
	}

	// 发送一条心跳消息
	logger.Infof("send heartbeat ping msg to LMQ Lookup(%s:%d)", peer.host, peer.port)
	return peer.doSendWithLook(protocol.PingID, pingBytes)
}

func (peer *lookupPeer) sendRegistration(unRegister bool, topicName, channelName string) (err error) {
	peer.cond.L.Lock()
	if peer.client == nil {
		// 没有连接lookup
		select {
		case peer.connectChan <- struct{}{}:
		default:
		}
		peer.cond.Wait() // 等待连接/连接失败
		if peer.client == nil {
			peer.cond.L.Unlock()
			return errors.New("lmq lookup server is not connected")
		}
	}
	defer peer.cond.L.Unlock()

	if len(topicName) == 0 {
		return errors.New("topic name can not be empty")
	}

	// 序列化数据
	var requestBody *protocol.RequestBody
	requestBody = &protocol.RequestBody{
		TopicName:   topicName,
		ChannelName: channelName,
	}
	data, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	// 发送消息
	if unRegister {
		return peer.doSendWithLook(protocol.UnRegisterID, data)
	}
	return peer.doSendWithLook(protocol.RegisterID, data)
}

func (peer *lookupPeer) getTopicChannels(topicName string) []string {
	err := peer.sendTopicChannels(topicName)
	if err != nil {
		return nil
	}

	var channels []string
	channelsResponseChan := peer.channelsResponseChan
	timer := time.NewTimer(defaultTimeout)

	select {
	case request := <-channelsResponseChan:
		if time.Now().Sub(request.sendAt) > defaultTimeout { // 超时，直接丢弃消息
			break
		}

		channels = request.data
	case <-peer.exitChan: // 已经关闭
	case <-timer.C:
		timer.Stop()
	}

	return channels
}

func (peer *lookupPeer) sendTopicChannels(topicName string) error {
	peer.cond.L.Lock()
	if peer.client == nil {
		// 没有连接lookup
		select {
		case peer.connectChan <- struct{}{}:
		default:
		}
		peer.cond.Wait() // 等待连接/连接失败
		if peer.client == nil {
			peer.cond.L.Unlock()
			return errors.New("lmq lookup server is not connected")
		}
	}
	defer peer.cond.L.Unlock()

	// 序列化数据
	var requestBody *protocol.RequestBody
	requestBody = &protocol.RequestBody{
		TopicName: topicName,
	}
	data, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	// 发送消息
	now := time.Now()
	err = peer.doSendWithLook(protocol.ChannelsID, data)
	if err != nil { // 在发送消息时错误，则进行重连
		select {
		case peer.connectChan <- struct{}{}:
		default:
		}
		peer.cond.L.Unlock()
		return err // 发送错误，返回空
	}

	peer.channelsRequestChan <- &channelsReq{
		topicName: topicName,
		sendAt:    now,
	}

	return nil
}

// 向lookup服务器发送消息，调用此函数时已经加锁了
func (peer *lookupPeer) doSendWithLook(id uint32, data []byte) (err error) {
	defer func() {
		if err != nil { // 出现了错误，进行重连
			select {
			case peer.connectChan <- struct{}{}:
			default:
			}
		}
	}()

	// 发送一条消息
	_ = peer.client.GetConnection().SendBufMsg(id, data)
	if err != nil {
		return
	}

	return nil
}
