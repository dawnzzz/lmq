package dirlock

import (
	"os"
	"path"
)

const lockFilename = "dir.lock"

type DirLock struct {
	dirPath  string // 需要加锁的路径
	lockFile *os.File
}

func NewDirLock(dirPath string) *DirLock {
	return &DirLock{
		dirPath: dirPath,
	}
}

// TryLock 尝试为dirPath上锁，如果加锁失败则返回false
func (l *DirLock) TryLock() bool {
	lockFile, err := os.OpenFile(path.Join(l.dirPath, lockFilename), os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return false // lock file已经存在，上锁失败
	}

	l.lockFile = lockFile
	return true // 加锁成功
}

// Unlock 为dirPath解锁，如果解锁失败则返回false
func (l *DirLock) Unlock() bool {
	if l.lockFile == nil {
		return false
	}

	_ = l.lockFile.Close()
	err := os.Remove(path.Join(l.lockFile.Name()))
	if err != nil {
		return false
	}

	return true
}
