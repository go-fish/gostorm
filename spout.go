package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Spout struct {
	Component
}

func NewSpout(pull, push string) (spout *Spout, err error) {
	spout = &Spout{}

	err = spout.InitReader(pull)
	if err != nil {
		return
	}

	err = spout.InitWriter(push)
	return
}

func (this *Spout) InitReader(pull string) (err error) {
	this.Component.ReadContext, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.ReadSocket, err = this.Component.ReadContext.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.ReadSocket.Connect("ipc://" + pull)
	return
}

func (this *Spout) InitWriter(push string) (err error) {
	this.Component.WriteContext, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.WriteSocket, err = this.Component.WriteContext.NewSocket(zmq.PULL)
	if err != nil {
		return
	}

	err = this.Component.WriteSocket.Bind("ipc://" + push)
	return
}
