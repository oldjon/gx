package gxtcp

import (
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oldjon/gutil/bytebuffer"
	"go.uber.org/zap"
)

const (
	cmdMaxSize    = 512 * 1024
	cmdVerifyTime = 5
)

type TCPFrame interface {
	HeaderSize() int
	Size([]byte) (int, error)
	EncodeMsg(uint8, uint32, interface{}) ([]byte, error)
	DecodeMsg([]byte, interface{}) error
}

type TCPTask struct {
	closed          int32
	verified        bool
	stoppedChan     chan struct{}
	revBuff         *bytebuffer.ByteBuffer
	sendBuff        *bytebuffer.ByteBuffer
	sendMutex       sync.Mutex
	sendChan        chan struct{}
	Conn            net.Conn
	logger          *zap.Logger
	parseMsgHandler func(data []byte)
	onCloseHandler  func()
	frame           TCPFrame
}

func NewTCPTask(conn net.Conn, logger *zap.Logger, pmHandler func(data []byte), ocHandler func(), f TCPFrame) *TCPTask {
	return &TCPTask{
		closed:          -1, // -1: initial stat, 0: open, 1: close
		verified:        false,
		Conn:            conn,
		logger:          logger,
		stoppedChan:     make(chan struct{}, 1),
		revBuff:         bytebuffer.NewByteBuffer(),
		sendBuff:        bytebuffer.NewByteBuffer(),
		sendChan:        make(chan struct{}, 1),
		parseMsgHandler: pmHandler,
		onCloseHandler:  ocHandler,
		frame:           f,
	}
}

func (tt *TCPTask) SendSignal() {
	select {
	case tt.sendChan <- struct{}{}:
	default:
	}
	return
}

func (tt *TCPTask) RemoteAddr() string {
	if tt.Conn == nil {
		return ""
	}
	return tt.Conn.RemoteAddr().String()
}

func (tt *TCPTask) LocalAddr() string {
	if tt.Conn == nil {
		return ""
	}
	return tt.Conn.LocalAddr().String()
}

func (tt *TCPTask) IsClosed() bool {
	return atomic.LoadInt32(&tt.closed) != 0
}

func (tt *TCPTask) Stop() bool {
	if tt.IsClosed() {
		tt.logger.Error("conn close failed ", zap.String("remote_addr", tt.RemoteAddr()))
		return false
	}
	select {
	case tt.stoppedChan <- struct{}{}:
	default:
		tt.logger.Error("conn close failed ", zap.String("remote_addr", tt.RemoteAddr()))
		return false
	}
	return true
}

func (tt *TCPTask) Start() {
	if !atomic.CompareAndSwapInt32(&tt.closed, -1, 0) {
		return
	}
	go tt.SendLoop()
	go tt.RevLoop()
	tt.logger.Info("conn received ", zap.String("remote_addr", tt.RemoteAddr()))
	return
}

func (tt *TCPTask) Close() {
	if tt == nil {
		return
	}
	if !atomic.CompareAndSwapInt32(&tt.closed, 0, 1) {
		return
	}
	tt.logger.Info("conn closed ", zap.String("remote_addr", tt.RemoteAddr()))
	_ = tt.Conn.Close()

	close(tt.stoppedChan)

	tt.revBuff.Reset()
	tt.sendBuff.Reset()
	tt.onCloseHandler()
	return
}

func (tt *TCPTask) Reset() bool {
	if atomic.LoadInt32(&tt.closed) != 1 {
		return false
	}
	if !tt.IsVerified() {
		return false
	}
	tt.closed = -1
	tt.verified = true
	tt.stoppedChan = make(chan struct{})
	tt.logger.Info("conn reset ", zap.String("remote_addr", tt.RemoteAddr()))
	return true
}

func (tt *TCPTask) Verify() {
	tt.verified = true
	return
}

func (tt *TCPTask) IsVerified() bool {
	return tt.verified
}

func (tt *TCPTask) Terminate() {
	tt.Close()
}

func (tt *TCPTask) SendMsg(cmd uint8, subCmd uint32, msg interface{}) bool {
	if tt.IsClosed() {
		return false
	}
	if tt.frame == nil {
		return false
	}

	buffer, err := tt.frame.EncodeMsg(cmd, subCmd, msg)
	if err != nil {
		tt.logger.Error("send msg: encode msg failed ", zap.Uint8("cmd", cmd), zap.Uint32("subcmd", subCmd), zap.Error(err))
		return false
	}
	tt.sendMutex.Lock()
	tt.sendBuff.Append(buffer...)
	tt.sendMutex.Unlock()
	tt.SendSignal()
	return true
}

func (tt *TCPTask) SendBytes(buffer []byte) bool {
	if tt.IsClosed() {
		return false
	}
	tt.sendMutex.Lock()
	tt.sendBuff.Append(buffer...)
	tt.sendMutex.Unlock()
	tt.SendSignal()
	return true
}

func (tt *TCPTask) readAtLeast(buff *bytebuffer.ByteBuffer, needNum int) error {
	buff.WriteGrow(needNum)
	n, err := io.ReadAtLeast(tt.Conn, buff.WriteBuf(), needNum)
	buff.WriteFlip(n)
	return err
}

func (tt *TCPTask) RevLoop() {
	defer func() {
		tt.Close()
		if err := recover(); err != nil {
			tt.logger.Error("panic ", zap.Any("error", err), zap.String("stack", string(debug.Stack())))
		}
	}()

	var (
		needNum   int
		err       error
		totalSize int
		dataSize  int
		msgBuff   []byte
	)

	for {
		select {
		case <-tt.stoppedChan:
			return
		default:
		}

		totalSize = tt.revBuff.ReadSize()
		headerSize := tt.frame.HeaderSize()
		if totalSize < headerSize {
			needNum = headerSize - totalSize
			err = tt.readAtLeast(tt.revBuff, needNum)
			if err != nil {
				tt.logger.Debug("conn read data failed ", zap.String("remote_addr", tt.RemoteAddr()), zap.Error(err))
				return
			}
			totalSize = tt.revBuff.ReadSize()
		}

		msgBuff = tt.revBuff.ReadBuf()

		dataSize, err = tt.frame.Size(msgBuff)
		if err != nil {
			tt.logger.Error("conn data smaller than min size 4", zap.String("remote_addr", tt.RemoteAddr()),
				zap.Int("data_size", dataSize))
			return
		}
		if dataSize > cmdMaxSize {
			tt.logger.Error("conn data larger than max size 128k", zap.String("remote_addr", tt.RemoteAddr()),
				zap.Int("data_size", dataSize))
			return
		} else if dataSize < headerSize {
			tt.logger.Error("conn data smaller than min size 4", zap.String("remote_addr", tt.RemoteAddr()),
				zap.Int("data_size", dataSize))
			return
		}

		if totalSize < dataSize {
			needNum = dataSize - totalSize
			err = tt.readAtLeast(tt.revBuff, needNum)
			if err != nil {
				tt.logger.Debug("conn read data failed ", zap.String("remote_addr", tt.RemoteAddr()), zap.Error(err))
				return
			}
			msgBuff = tt.revBuff.ReadBuf()
		}
		tt.parseMsgHandler(msgBuff[:dataSize])
		tt.revBuff.ReadFlip(dataSize)
	}
}

func (tt *TCPTask) SendLoop() {
	defer func() {
		tt.Close()
		if err := recover(); err != nil {
			tt.logger.Error("panic ", zap.Any("error", err), zap.String("stack", string(debug.Stack())))
		}
	}()

	var (
		tmpByte  = bytebuffer.NewByteBuffer()
		timeout  = time.NewTimer(time.Second * cmdVerifyTime)
		writeNum int
		err      error
	)

	defer timeout.Stop()

	for {
		select {
		case <-tt.sendChan:
			for {
				tt.sendMutex.Lock()
				if tt.sendBuff.ReadReady() {
					tmpByte.Append(tt.sendBuff.ReadBuf()...)
					tt.sendBuff.Reset()
				}
				tt.sendMutex.Unlock()

				if !tmpByte.ReadReady() {
					break
				}

				writeNum, err = tt.Conn.Write(tmpByte.ReadBuf())
				if err != nil {
					tt.logger.Error("conn send data failed ", zap.String("remote_addr", tt.RemoteAddr()), zap.Error(err))
					return
				}
				tmpByte.ReadFlip(writeNum)
			}
		case <-tt.stoppedChan:
			return
		case <-timeout.C:
			if !tt.IsVerified() {
				tt.logger.Error("conn verify timeout ", zap.String("remote_addr", tt.RemoteAddr()))
				return
			}
		}
	}
}
