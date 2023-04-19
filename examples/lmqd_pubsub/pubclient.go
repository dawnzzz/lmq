package main

import (
	"encoding/json"
	"github.com/dawnzzz/hamble-tcp-server/hamble"
	"github.com/dawnzzz/lmq/examples/lmqd_pubsub/handler"
	"github.com/dawnzzz/lmq/internel/protocol"
	"time"
)

func main() {
	client, err := hamble.NewClient("tcp", "127.0.0.1", 6200)
	if err != nil {
		return
	}

	client.RegisterHandler(protocol.CreateChannelID, &handler.RecvHandler{})
	client.RegisterHandler(protocol.PubID, &handler.RecvHandler{})

	go client.Start()

	request := protocol.RequestBody{
		TopicName:   "test_topic",
		ChannelName: "test_channel",
		MessageData: []byte("hello"),
	}
	data, _ := json.Marshal(request)

	_ = client.GetConnection().SendBufMsg(protocol.CreateChannelID, data)
	_ = client.GetConnection().SendBufMsg(protocol.PubID, data)

	time.Sleep(2 * time.Second)
}
