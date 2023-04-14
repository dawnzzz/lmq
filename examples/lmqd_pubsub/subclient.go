package main

import (
	"encoding/json"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	"github.com/dawnzzz/lmq/examples/lmqd_pubsub/handler"
	"github.com/dawnzzz/lmq/lmqd/tcp"
)

func main() {
	client, err := hamble.NewClient("tcp", "127.0.0.1", 6200)
	if err != nil {
		return
	}
	client.RegisterHandler(tcp.SubID, &handler.RecvHandler{})
	client.RegisterHandler(tcp.RydID, &handler.RecvHandler{})
	client.RegisterHandler(tcp.SendMsgID, &handler.RecvMessageHandler{})

	go client.Start()

	data, _ := json.Marshal(tcp.RequestBody{
		TopicName:   "test_topic",
		ChannelName: "test_channel",
	})
	_ = client.GetConnection().SendBufMsg(tcp.SubID, data)

	data, _ = json.Marshal(tcp.RequestBody{
		Count: 10,
	})
	_ = client.GetConnection().SendBufMsg(tcp.RydID, data)

	select {}
}
