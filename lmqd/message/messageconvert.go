package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"github.com/dawnzzz/lmq/iface"
	"io"
	"sync"
)

var bp sync.Pool

func init() {
	bp.New = func() interface{} {
		return &bytes.Buffer{}
	}
}

func bufferPoolGet() *bytes.Buffer {
	return bp.Get().(*bytes.Buffer)
}

func bufferPoolPut(b *bytes.Buffer) {
	b.Reset()
	bp.Put(b)
}

func ConvertMessageToBytes(message iface.IMessage) ([]byte, error) {
	length := int(message.GetLength())

	var err error
	buffer := bufferPoolGet()
	defer bufferPoolPut(buffer)

	buffer.Grow(length)

	// 写入ID
	_, err = buffer.Write(message.GetID().Bytes())
	if err != nil {
		return nil, err
	}

	// 写入时间戳
	err = binary.Write(buffer, binary.BigEndian, message.GetTimestamp())
	if err != nil {
		return nil, err
	}

	// 写入attempts
	err = binary.Write(buffer, binary.BigEndian, message.GetAttempts())
	if err != nil {
		return nil, err
	}

	// 写入数据
	_, err = buffer.Write(message.GetData())
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

var errConvertFailed = errors.New("convert bytes to message err")

func ConvertBytesToMessage(data []byte) (iface.IMessage, error) {
	reader := bytes.NewReader(data)

	// 读取ID
	idSlice := make([]byte, iface.MsgIDLength)
	n, err := reader.Read(idSlice)
	if err != nil {
		return nil, err
	}
	if n != iface.MsgIDLength {
		return nil, errConvertFailed
	}
	msgID, err := sliceToMessageID(idSlice)
	if err != nil {
		return nil, err
	}

	// 读取时间戳
	var timestamp int64
	err = binary.Read(reader, binary.BigEndian, &timestamp)
	if err != nil {
		return nil, err
	}

	// 读取attempts
	var attempts uint16
	err = binary.Read(reader, binary.BigEndian, &attempts)
	if err != nil {
		return nil, err
	}

	// 读取message数据
	buffer := bufferPoolGet()
	defer bufferPoolPut(buffer)
	_, err = buffer.ReadFrom(reader)
	if err != nil && err != io.EOF {
		return nil, err
	}

	msg := NewMessage(*msgID, buffer.Bytes())
	msg.SetTimestamp(timestamp)
	msg.SetAttempts(attempts)

	return msg, nil
}

func sliceToMessageID(slice []byte) (*iface.MessageID, error) {
	if len(slice) != iface.MsgIDLength {
		return nil, errConvertFailed
	}

	msgID := iface.MessageID{}
	for i, b := range slice {
		msgID[i] = b
	}

	return &msgID, nil
}
