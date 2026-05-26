package pubsub

type AckType int

const (
	Ack = iota
	NackRequeue
	NackDiscard
)
