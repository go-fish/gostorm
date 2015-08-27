package gostorm

import (
	"bufio"
	"bytes"
	"os"

	zmq "github.com/pebbe/zmq4"
)

type Spout struct {
	Component
}

func NewSpout(pull, push string) (spout *Spout, err error) {
	spout = &Spout{}

	var reader = bufio.NewReader(os.Stdin)
	var info []byte

	info, _, err = reader.ReadLine()
	if err != nil {
		return
	}

	var index = bytes.IndexByte(info, '\t')
	var pull = bytesToInt(info[0:index])
	var push = bytesToInt(info[index+1:])

	err = spout.InitSocket(pull, push)
	return
}

func (this *Spout) InitSocket(pull, push string) (err error) {
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
