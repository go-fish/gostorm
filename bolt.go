package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Bolt struct {
	Component
}

func NewBolt(pull, push string) (bolt *Bolt, err error) {
	bolt = &Bolt{}

	err = bolt.InitSocket(pull, push)
	return
}

func (this *Bolt) InitSocket(pull, push string) (err error) {
	this.Component.Context, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.Reader, err = this.Component.Context.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.Reader.Connect("tcp://" + pull)
	if err != nil {
		return
	}

	this.Component.Writer, err = this.Component.Context.NewSocket(zmq.PUSH)
	if err != nil {
		return
	}

	err = this.Component.Writer.Bind("tcp://" + push)
	return
}
