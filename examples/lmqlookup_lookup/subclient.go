package main

import (
	"encoding/json"
	"fmt"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	serveriface "github.com/dawnzzz/hamble-tcp-server/iface"
	"github.com/dawnzzz/lmq/examples/lmqd_pubsub/handler"
	"github.com/dawnzzz/lmq/internel/protocol"
)

var (
	nodes       []*protocol.Node
	nodesFinish = make(chan struct{})
)

type nodesRecvHandler struct {
	hamble.BaseHandler
}

func (h *nodesRecvHandler) Handle(request serveriface.IRequest) {
	data := request.GetData()
	resp := &protocol.ResponseBody{}
	_ = json.Unmarshal(data, resp)
	if resp.Nodes != nil {
		for _, node := range resp.Nodes {
			fmt.Printf("%#v\n", node)
		}

		nodes = append(nodes, resp.Nodes...)
		close(nodesFinish)
	}

}

type topicsRecvHandler struct {
	hamble.BaseHandler
}

func (h *topicsRecvHandler) Handle(request serveriface.IRequest) {
	data := request.GetData()
	resp := &protocol.ResponseBody{}
	_ = json.Unmarshal(data, resp)
	fmt.Printf("%#v\n", resp)
}

func main() {
	// 连接Lmq Lookup
	lookupClient, err := hamble.NewClient("tcp", "127.0.0.1", 6300)
	if err != nil {
		return
	}
	lookupClient.RegisterHandler(protocol.NodesID, &nodesRecvHandler{})
	lookupClient.RegisterHandler(protocol.TopicsID, &topicsRecvHandler{})
	go lookupClient.Start()

	var requestBody protocol.RequestBody
	var data []byte

	println(lookupClient.GetConnection().GetConn().LocalAddr().String())

	// 发送nodes消息，获取节点信息
	requestBody = protocol.RequestBody{}
	data, _ = json.Marshal(requestBody)
	err = lookupClient.GetConnection().SendBufMsg(protocol.NodesID, data)
	if err != nil {
		fmt.Printf("%#v\n", err)
	}

	requestBody = protocol.RequestBody{}
	data, _ = json.Marshal(requestBody)
	err = lookupClient.GetConnection().SendBufMsg(protocol.TopicsID, data)
	if err != nil {
		fmt.Printf("%#v\n", err)
	}

	// 连接lmqd
	<-nodesFinish
	if len(nodes) > 0 {
		// 连接第一个节点
		node := nodes[0]
		fmt.Printf("select lmqd %s:%d\n", node.Hostname, node.TCPPort)
		lmqdClient, err := hamble.NewClient("tcp", node.Hostname, node.TCPPort)
		if err != nil {
			return
		}

		lmqdClient.RegisterHandler(protocol.SubID, &handler.RecvHandler{})
		lmqdClient.RegisterHandler(protocol.RydID, &handler.RecvHandler{})
		lmqdClient.RegisterHandler(protocol.SendMsgID, &handler.RecvMessageHandler{})

		go lmqdClient.Start()

		data, _ = json.Marshal(protocol.RequestBody{
			TopicName:   "test_topic",
			ChannelName: "test_channel",
		})
		fmt.Printf("%s\n", data)
		_ = lmqdClient.GetConnection().SendBufMsg(protocol.SubID, data)

		data, _ = json.Marshal(protocol.RequestBody{
			Count: 10,
		})
		_ = lmqdClient.GetConnection().SendBufMsg(protocol.RydID, data)
	} else {
		fmt.Println("no lmqd")
	}

	select {}
}
