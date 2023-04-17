package backendqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/dawnzzz/lmq/logger"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"
)

// DiskBackendQueue 磁盘队列
type DiskBackendQueue struct {
	name                string        // 名字
	dataPath            string        // 数据路径
	maxBytesPerFile     int64         // 每一个文件的最大长度
	maxBytesPerFileRead int64         // 当前读取的文件的最大长度
	minMsgSize          int32         // 消息的最小长度
	maxMsgSize          int32         // 消息的最大长度
	syncEvery           int64         // 多少次次读写操作后进行fsync同步操作和同步元数据信息操作
	syncTimeout         time.Duration // 最长多长时间进行一次同步操作
	isExiting           bool
	needSync            bool

	sync.RWMutex

	readFileIndex  int64 // 读取的文件号
	readFilePos    int64 // 读取的文件位置
	writeFileIndex int64 // 写入的文件号
	writeFilePos   int64 // 写入的文件位置

	nextReadPos       int64 // 下一次要读取的位置
	nextReadFileIndex int64 // 下一次尧都区的文件号

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	readChan chan []byte

	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan struct{}
	emptyResponseChan chan error
	exitChan          chan struct{}
	exitSyncChan      chan struct{}
}

func NewDiskBackendQueue(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration) BackendQueue {

	queue := &DiskBackendQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan struct{}),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan struct{}),
		exitSyncChan:      make(chan struct{}),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
	}

	// 读取元数据，此时读取了读取和写入的文件号、以及位置pos
	err := queue.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		logger.Errorf("DiskQueue(%s) failed to retrieveMetaData - %s", queue.name, err.Error())
	}

	// 开启一个协程，进行ioLoop
	go queue.ioLoop()

	return queue
}

func (queue *DiskBackendQueue) Put(data []byte) error {
	queue.RLock()
	defer queue.RUnlock()

	if queue.isExiting {
		return errors.New("exiting")
	}

	queue.writeChan <- data
	return <-queue.writeResponseChan
}

func (queue *DiskBackendQueue) ReadChan() <-chan []byte {
	return queue.readChan
}

func (queue *DiskBackendQueue) Close() error {
	err := queue.exit(false)
	if err != nil {
		return err
	}
	return queue.sync()
}

func (queue *DiskBackendQueue) Delete() error {
	return queue.exit(true)
}

func (queue *DiskBackendQueue) exit(deleted bool) error {
	queue.Lock()
	defer queue.Unlock()

	queue.isExiting = true

	if deleted {
		logger.Infof("DiskQueue is deleting", queue.name)
	} else {
		logger.Infof("DiskQueue is closing", queue.name)
	}

	close(queue.exitChan)
	<-queue.exitSyncChan

	if queue.readFile != nil {
		_ = queue.readFile.Close()
		queue.readFile = nil
	}

	if queue.writeFile != nil {
		_ = queue.writeFile.Close()
		queue.writeFile = nil
	}

	return nil
}

func (queue *DiskBackendQueue) Empty() error {
	queue.RLock()
	defer queue.RUnlock()

	if queue.isExiting {
		return errors.New("exiting")
	}

	logger.Infof("DiskQueue is emptying", queue.name)

	queue.emptyChan <- struct{}{}

	return <-queue.emptyResponseChan
}

// retrieveMetaData 检索元数据
func (queue *DiskBackendQueue) retrieveMetaData() error {
	// 首先获取元数据文件名，并且读取文件
	metaFilename := queue.metaDataFileName()
	f, err := os.OpenFile(metaFilename, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// 读取元数据文件内容
	_, err = fmt.Fscanf(f, "%d,%d\n%d,%d\n", &queue.readFileIndex, &queue.readFilePos, &queue.writeFileIndex, &queue.writeFilePos)
	if err != nil {
		return err
	}

	queue.nextReadFileIndex = queue.readFileIndex
	queue.nextReadPos = queue.readFilePos

	// 获取文件长度，检查write file pos是否合理
	fileName := queue.fileName(queue.writeFileIndex)
	fileInfo, err := os.Stat(fileName)
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()
	if queue.writeFilePos < fileSize {
		// write pos 小于文件长度，则跳到下一个文件进行写入
		logger.Warnf("DISKQUEUE(%s) %s metadata writePos %d < file size of %d, skipping to new file",
			queue.name, fileName, queue.writeFilePos, fileSize)
		queue.writeFileIndex += 1
		queue.writeFilePos = 0
		if queue.writeFile != nil {
			_ = queue.writeFile.Close()
			queue.writeFile = nil
		}
	}

	return nil
}

func (queue *DiskBackendQueue) persistMetaData() error {
	fileName := queue.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp.data", fileName, rand.Int())

	// write to tmp file
	f, err := os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	_, err = fmt.Fprintf(f, "%d,%d\n%d,%d\n", queue.readFileIndex, queue.readFilePos, queue.writeFileIndex, queue.writeFilePos)
	if err != nil {
		_ = f.Close()
		return err
	}

	_ = f.Sync()
	_ = f.Close()

	return os.Rename(tmpFileName, fileName)
}

// disk queue的核心函数，用于读写
func (queue *DiskBackendQueue) ioLoop() {
	var err error
	var count int64
	var readChan chan []byte
	var dataRead []byte

	syncTicker := time.NewTicker(queue.syncTimeout)

	for {
		if count == queue.syncEvery {
			queue.needSync = true
		}

		// 需要fsync
		if queue.needSync {
			err = queue.sync()
			if err != nil {
				logger.Errorf("DiskQueue(%s) failed to sync - %v", queue.name, err)
			}
			count = 0
		}

		// 有消息还未读取，那么就读取一个消息
		if (queue.readFileIndex < queue.writeFileIndex) || (queue.readFilePos < queue.writeFilePos) {
			if queue.nextReadPos == queue.readFilePos {
				dataRead, err = queue.readOne()
				if err != nil {
					logger.Errorf("DiskQueue(%s) reading at %d of %s - %s", queue.name, queue.readFilePos, queue.fileName(queue.readFileIndex), err.Error())
					queue.handleReadError() // 发生读取错误就取消当前文件的写入和读取，转到下一个文件写入。
					continue
				}
				readChan = queue.readChan
			} else {
				readChan = nil
			}
		}

		select {
		case data := <-queue.writeChan: // 有写入请求
			count++
			queue.writeResponseChan <- queue.writeOne(data)
		case readChan <- dataRead: // 有读取请求
			count++
			queue.moveForward()
		case <-queue.emptyChan: // 有清空请求
			queue.emptyResponseChan <- queue.deleteAllFiles()
			count = 0
		case <-syncTicker.C:
			if count == 0 {
				// 期间没有进行读写操作，跳过同步
				continue
			}
			queue.needSync = true
		case <-queue.exitChan:
			goto exit
		}
	}

exit:
	logger.Infof("DiskQueue(%s) ioLoop closing", queue.name)
	syncTicker.Stop()
	queue.exitSyncChan <- struct{}{}
}

func (queue *DiskBackendQueue) readOne() ([]byte, error) {
	var err error

	// 读取文件并seek
	if queue.readFile == nil {
		// 读取文件
		curFilename := queue.fileName(queue.readFileIndex)
		queue.readFile, err = os.OpenFile(curFilename, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		// seek
		if queue.readFilePos > 0 {
			_, err = queue.readFile.Seek(queue.readFilePos, 0)
			if err != nil {
				_ = queue.readFile.Close()
				queue.readFile = nil
				return nil, err
			}
		}

		queue.maxBytesPerFileRead = queue.maxBytesPerFile
		if queue.readFileIndex < queue.writeFileIndex {
			stat, err := queue.readFile.Stat()
			if err == nil {
				queue.maxBytesPerFileRead = stat.Size()
			}
		}

		queue.reader = bufio.NewReader(queue.readFile)
	}

	// 读取消息长度
	var msgSize int32
	err = binary.Read(queue.reader, binary.BigEndian, &msgSize)
	if err != nil {
		_ = queue.readFile.Close()
		queue.readFile = nil
		return nil, err
	}

	if msgSize < queue.minMsgSize || msgSize > queue.maxMsgSize {
		_ = queue.readFile.Close()
		queue.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	// 读取消息数据
	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(queue.reader, readBuf)
	if err != nil {
		_ = queue.readFile.Close()
		queue.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)
	queue.nextReadPos = queue.readFilePos + totalBytes
	queue.nextReadFileIndex = queue.readFileIndex

	if queue.readFileIndex < queue.writeFileIndex && queue.nextReadPos > queue.maxBytesPerFileRead {
		if queue.readFile != nil {
			_ = queue.readFile.Close()
			queue.readFile = nil
		}

		queue.nextReadFileIndex++
		queue.nextReadPos = 0
	}

	return readBuf, nil
}

func (queue *DiskBackendQueue) writeOne(data []byte) error {
	var err error

	dataLen := int32(len(data))
	totalBytes := int64(4 + dataLen)

	if dataLen < queue.minMsgSize || dataLen > queue.maxMsgSize {
		// 消息不符合长度
		return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", dataLen, queue.minMsgSize, queue.maxMsgSize)
	}

	// 如果加入这条消息超过了最大文件长度，那么就写入下一个文件中
	if queue.writeFilePos > 0 && queue.writeFilePos+totalBytes > queue.maxBytesPerFile {
		if queue.readFileIndex == queue.writeFileIndex {
			// 若读取和写入的是同一个文件，则最大读取长度为当前文件长度
			queue.maxBytesPerFileRead = queue.writeFilePos
		}

		queue.writeFileIndex++
		queue.writeFilePos = 0

		// 同步meta信息
		err = queue.sync()
		if err != nil {
			logger.Errorf("DiskQueue(%s) failed to sync - %v", queue.name, err)
		}

		if queue.writeFile != nil {
			_ = queue.writeFile.Close()
			queue.writeFile = nil
		}
	}

	if queue.writeFile == nil {
		curFileName := queue.fileName(queue.writeFileIndex)
		queue.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		if queue.writeFilePos > 0 {
			_, err = queue.writeFile.Seek(queue.writeFilePos, 0)
			if err != nil {
				_ = queue.writeFile.Close()
				queue.writeFile = nil
				return err
			}
		}

	}

	// 向文件中分别写入数据长度和数据
	queue.writeBuf.Reset()
	err = binary.Write(&queue.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = queue.writeBuf.Write(data)
	if err != nil {
		return err
	}

	_, err = queue.writeFile.Write(queue.writeBuf.Bytes())
	if err != nil {
		_ = queue.writeFile.Close()
		queue.writeFile = nil
		return err
	}

	queue.writeFilePos += totalBytes

	return nil
}

func (queue *DiskBackendQueue) metaDataFileName() string {
	return fmt.Sprintf("%s.diskqueue.meta.dat", queue.name)
}

func (queue *DiskBackendQueue) fileName(index int64) string {
	return fmt.Sprintf("%s.diskqueue.%06d.dat", queue.name, index)
}

// sync fsync和同步元数据信息
func (queue *DiskBackendQueue) sync() error {
	if queue.writeFile != nil {
		err := queue.writeFile.Sync()
		if err != nil {
			_ = queue.writeFile.Close()
			queue.writeFile = nil
			return err
		}
	}

	// 持久化meta信息
	err := queue.persistMetaData()
	if err != nil {
		return err
	}

	queue.needSync = false
	return nil
}

func (queue *DiskBackendQueue) handleReadError() {
	if queue.readFileIndex == queue.writeFileIndex {
		// 停止当前文件的写入，直接开启新的文件进行写入
		if queue.writeFile != nil {
			_ = queue.writeFile.Close()
			queue.writeFile = nil
		}
		queue.writeFileIndex++
		queue.writeFilePos = 0
	}

	// 读取错误重命名文件
	badFilename := queue.fileName(queue.readFileIndex)
	badRenameFilename := badFilename + ".bad"

	logger.Warnf("DiskQueue(%s) jump to next file and saving bad file as %s", queue.name, badRenameFilename)

	err := os.Rename(badFilename, badRenameFilename)
	if err != nil {
		logger.Errorf("DiskQueue(%s) failed rename file from %s to %s", queue.name, badFilename, badRenameFilename)
	}

	queue.readFileIndex++
	queue.readFilePos = 0
	queue.nextReadFileIndex = queue.readFileIndex
	queue.nextReadPos = queue.readFilePos

	queue.needSync = true
}

func (queue *DiskBackendQueue) moveForward() {
	oldReadFileIndex := queue.readFileIndex
	queue.readFileIndex = queue.nextReadFileIndex
	queue.readFilePos = queue.nextReadPos

	if oldReadFileIndex != queue.readFileIndex {
		// 已经移动到了下一个文件进行读取，删除前面的文件
		queue.needSync = true

		filename := queue.fileName(oldReadFileIndex)
		err := os.Remove(filename)
		if err != nil {
			logger.Errorf("DiskQueue(%s) failed to Remove(%s) - %s", queue.name, filename, err.Error())
		}
	}
}

func (queue *DiskBackendQueue) deleteAllFiles() error {
	err := queue.skipToNextRWFile()

	// 删除元数据
	innerErr := os.Remove(queue.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		logger.Errorf("DISKQUEUE(%s) failed to remove metadata file - %s", queue.name, innerErr)
		return innerErr
	}

	return err
}

func (queue *DiskBackendQueue) skipToNextRWFile() error {
	var err error

	if queue.readFile != nil {
		_ = queue.readFile.Close()
		queue.readFile = nil
	}

	if queue.writeFile != nil {
		_ = queue.writeFile.Close()
		queue.writeFile = nil
	}

	for i := queue.readFileIndex; i <= queue.writeFileIndex; i++ {
		fn := queue.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			logger.Errorf("DISKQUEUE(%s) failed to remove data file - %s", queue.name, innerErr)
			err = innerErr
		}
	}

	queue.writeFileIndex++
	queue.writeFilePos = 0
	queue.readFileIndex = queue.writeFileIndex
	queue.readFilePos = 0
	queue.nextReadFileIndex = queue.writeFileIndex
	queue.nextReadPos = 0

	return err
}
