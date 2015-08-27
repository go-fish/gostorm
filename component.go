package gostorm

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/golang/protobuf/proto"
	zmq "github.com/pebbe/zmq4"
)

type Component struct {
	Reader *zmq.Socket
	Writer *zmq.Socket
}

func (this *Component) Ack(id string) (err error) {
	var ack = &ShellMsg{
		Command: proto.String("ack"),
		Id:      proto.String(id),
	}

	var info []byte
	info, err = proto.Marshal(ack)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) Fail(id string) (err error) {
	var fail = &ShellMsg{
		Command: proto.String("fail"),
		Id:      proto.String(id),
	}

	var info []byte
	info, err = proto.Marshal(fail)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) Log(msg string) (err error) {
	var log = &ShellMsg{
		Command: proto.String("log"),
		Msg:     proto.String(msg),
	}

	var info []byte
	info, err = proto.Marshal(log)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) Sync() (err error) {
	var sync = &Command{
		Command: proto.String("sync"),
	}

	var info []byte
	info, err = proto.Marshal(sync)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) SpoutEmit(msg []string, options map[string]string) (err error) {
	var emit = &ShellMsg{
		Command: proto.String("emit"),
		Tuple:   msg,
	}

	if options != nil {
		if val, ok := options["id"]; ok {
			emit.Id = proto.String(val)
		}

		if val, ok := options["stream"]; ok {
			emit.Stream = proto.String(val)
		}

		if val, ok := options["task"]; ok {
			task, _ := strconv.Atoi(val)
			emit.Task = proto.Int64(int64(task))
		}
	}

	var info []byte
	info, err = proto.Marshal(emit)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) BoltEmit(msg, anchors []string, options map[string]string) (err error) {
	var emit = &ShellMsg{
		Command: proto.String("emit"),
		Anchors: anchors,
		Tuple:   msg,
	}

	if options != nil {
		if val, ok := options["stream"]; ok {
			emit.Stream = proto.String(val)
		}

		if val, ok := options["task"]; ok {
			task, _ := strconv.Atoi(val)
			emit.Task = proto.Int64(int64(task))
		}
	}

	var info []byte
	info, err = proto.Marshal(emit)
	if err != nil {
		return
	}

	return this.sendMsg(info)
}

func (this *Component) ReadMsg() ([]byte, error) {
	return this.readMsg()
}

func (this *Component) Handshake() (err error) {
	var info []byte
	info, err = this.readMsg()
	if err != nil {
		return err
	}

	var init = &Init{}
	err = proto.Unmarshal(info, init)
	if err != nil {
		return
	}

	var pidDir string
	var pid int
	pidDir = init.GetPidDir()
	pid = os.Getpid()

	var file *os.File
	file, err = os.Create(filepath.Join(pidDir, strconv.Itoa(pid)))
	file.Close()
	if err != nil {
		return
	}

	var p = &Pid{
		Pid: proto.Int64(int64(pid)),
	}

	var buff []byte
	buff, err = proto.Marshal(p)
	if err != nil {
		return
	}

	return this.sendMsg(buff)
}

func (this *Component) readMsg() ([]byte, error) {
	return this.Reader.RecvBytes(0)
}

func (this *Component) sendMsg(info []byte) (err error) {
	_, err = this.Writer.SendBytes(info, 0)
	return
}
