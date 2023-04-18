package lmqd

import (
	"encoding/json"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/internel/utils"
	"github.com/dawnzzz/lmq/logger"
	"io"
	"os"
	"path"
)

// MetaData 元数据，记录了topic channel信息
type MetaData struct {
	Topics []*TopicMetaData `json:"topics"`
}

type TopicMetaData struct {
	Name      string             `json:"name,required"`
	IsPausing bool               `json:"is_pausing"`
	Channels  []*ChannelMetaData `json:"channels,omitempty"`
}

type ChannelMetaData struct {
	Name      string `json:"name,required"`
	IsPausing bool   `json:"is_pausing"`
}

func (lmqd *LmqDaemon) metaFilename() string {
	return path.Join(config.GlobalLmqdConfig.DataRootPath, "[lmqd].meta.dat")
}

// LoadMetaData 加载元数据信息
func (lmqd *LmqDaemon) LoadMetaData() error {
	if !lmqd.isLoading.CompareAndSwap(false, true) {
		// 已经在加载元数据信息了，直接返回
		return nil
	}
	defer lmqd.isLoading.Store(false)

	// 打开元数据文件
	metaFilename := lmqd.metaFilename()
	metaFile, err := os.OpenFile(metaFilename, os.O_RDONLY, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		return err
	}
	defer metaFile.Close()

	// 读取元数据文件
	data, err := io.ReadAll(metaFile)
	if err != nil {
		return err
	}

	// 反序列化，转为结构体
	var metaData MetaData
	err = json.Unmarshal(data, &metaData)
	if err != nil {
		return err
	}

	// 加载元数据信息
	for _, topicMetaData := range metaData.Topics {
		if !utils.TopicOrChannelNameIsValid(topicMetaData.Name) { // topic名字不合法，直接跳过
			continue
		}

		// 加载topic
		t, err := lmqd.GetTopic(topicMetaData.Name)
		if err != nil {
			logger.Errorf("get topic(%s) err when retrieveMetaData, err: %s", topicMetaData.Name, err.Error())
			continue
		}

		t.Start()

		if topicMetaData.IsPausing {
			_ = t.Pause()
		}

		// 开始加载topic内的channel
		for _, channelMetaData := range topicMetaData.Channels {
			if !utils.TopicOrChannelNameIsValid(channelMetaData.Name) { // channel名字不合法，直接跳过
				continue
			}

			// 加载channel
			c, err := t.GetChannel(channelMetaData.Name)
			if err != nil {
				logger.Errorf("get topic(%s) channel(%s) err when retrieveMetaData, err: %s", topicMetaData.Name, channelMetaData.Name, err.Error())
				continue
			}

			if channelMetaData.IsPausing {
				_ = c.Pause()
			}
		}
	}

	return nil
}

// PersistMetaData 持久化元数据信息
func (lmqd *LmqDaemon) PersistMetaData() error {
	lmqd.topicsLock.Lock()
	defer lmqd.topicsLock.Unlock()

	if len(lmqd.topics) <= 0 {
		return nil
	}

	var metaData MetaData

	// 持久化操作
	for _, t := range lmqd.topics {
		topicMetaData := TopicMetaData{
			Name:      t.GetName(),
			IsPausing: t.IsPausing(),
		}

		// 获取所有的channel name
		channelNames := t.GetChannelNames()
		for _, channelName := range channelNames {
			c, err := t.GetExistingChannel(channelName)
			if err != nil {
				continue
			}

			topicMetaData.Channels = append(topicMetaData.Channels, &ChannelMetaData{
				Name:      c.GetName(),
				IsPausing: c.IsPausing(),
			})
		}

		metaData.Topics = append(metaData.Topics, &topicMetaData)
	}

	// 序列化并保存在磁盘中
	data, err := json.Marshal(&metaData)
	if err != nil {
		return err
	}

	// 打开元数据文件，并写入元数据
	metaFilename := lmqd.metaFilename()
	metaFile, err := os.OpenFile(metaFilename, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer metaFile.Close()

	_, err = metaFile.Write(data)
	if err != nil {
		return err
	}

	return nil
}
