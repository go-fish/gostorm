package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Bolt struct {
	Component
}

func NewBolt(pull, push string) (bolt *Bolt, err error) {
	bolt = &Bolt{}

	err = bolt.InitReader(pull)
	if err != nil {
		return
	}

	err = bolt.InitWriter(push)
	return
}

func (this *Bolt) InitReader(pull string) (err error) {
	this.Component.ReadContext, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.ReadSocket, err = this.Component.ReadContext.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.ReadSocket.Connect("tcp://" + pull)
	return
}

func (this *Bolt) InitWriter(push string) (err error) {
	this.Component.WriteContext, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.WriteSocket, err = this.Component.WriteContext.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.WriteSocket.Bind("tcp://" + push)
	return
}
