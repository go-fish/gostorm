package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Spout struct {
	Component
}

func NewSpout(pair string) (spout *Spout, err error) {
	spout = &Spout{}

	err = spout.InitSocket(push)
	return
}

func (this *Spout) InitSocket(pair string) (err error) {
	this.Component.Context, err = zmq.NewContext()
	if err != nil {
		return
	}

	this.Component.Socket, err = this.Component.Context.NewSocket(zmq.PAIR)
	if err != nil {
		return
	}

	err = this.Component.Socket.Connect("tcp://" + pair)
	return
}
