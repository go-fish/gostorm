package gostorm

import (
	zmq "github.com/pebbe/zmq4"
)

type Spout struct {
	Component
}

func NewSpout(pull, push string) (spout *Spout, err error) {
	spout = &Spout{}

	err = spout.InitSocket(pull, push)
	return
}

func (this *Spout) InitSocket(pull, push string) (err error) {
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
