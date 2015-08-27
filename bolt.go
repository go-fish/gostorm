package gostorm

import (
	"bufio"
	"bytes"
	"os"

	zmq "github.com/pebbe/zmq4"
)

type Bolt struct {
	Component
}

func NewBolt(pull, push string) (bolt *Bolt, err error) {
	bolt = &Bolt{}

	var reader = bufio.NewReader(os.Stdin)
	var info []byte
	var err error

	info, _, err = reader.ReadLine()
	if err != nil {
		return
	}

	var index = bytes.IndexByte(info, '\t')
	var pull = bytesToInt(info[0:index])
	var push = bytesToInt(info[index+1:])

	err = bolt.InitSocket(pull, push)
	return
}

func (this *Bolt) InitSocket(pull, push string) (err error) {
	var reader, writer *zmq.Context
	reader, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.Reader, err = reader.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.Reader.Connect("ipc://" + pull)
	if err != nil {
		return
	}

	writer, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.Writer, err = writer.NewSocket(zmq.PUSH)
	if err != nil {
		return
	}

	err = this.Component.Writer.Bind("ipc://" + push)
	return
}
