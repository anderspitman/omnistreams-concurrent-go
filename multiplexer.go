package omniconc

import (
        //"fmt"
        "log"
        "github.com/anderspitman/omnistreams-core-go"
)

const MESSAGE_TYPE_CREATE_RECEIVE_STREAM = 0
const MESSAGE_TYPE_STREAM_DATA = 1
const MESSAGE_TYPE_STREAM_END = 2
const MESSAGE_TYPE_CANCEL_STREAM = 3
const MESSAGE_TYPE_STREAM_REQUEST_DATA = 4
const MESSAGE_TYPE_CONTROL_MESSAGE = 5

func CreateMultiplexer() Multiplexer {
        return Multiplexer{
                receiveStreams: make(map[byte]*ReceiveStream),
        }
}

type Multiplexer struct {
        send func([]byte)
        receiveStreams map[byte]*ReceiveStream
        conduitCallback func(producer omnicore.Producer, metadata []byte)
        controlMessageCallback func(message []byte)
}

func (m *Multiplexer) OnControlMessage(callback func(message []byte)) {
        m.controlMessageCallback = callback
}

func (m *Multiplexer) SendControlMessage(message []byte) {
        var reqMsg [1]byte
        reqMsg[0] = MESSAGE_TYPE_CONTROL_MESSAGE
        m.send(append(reqMsg[:], message...))
}

func (m *Multiplexer) HandleMessage(message []byte) {
        messageType := message[0]

        if messageType == MESSAGE_TYPE_CONTROL_MESSAGE {
                m.controlMessageCallback(message[1:])
        } else {
                streamId := message[1]
                switch messageType {
                case MESSAGE_TYPE_CREATE_RECEIVE_STREAM:
                        producer := ReceiveStream{}
                        producer.upstreamRequest = func(numElements uint32) {
                                var reqMsg [3]byte
                                reqMsg[0] = MESSAGE_TYPE_STREAM_REQUEST_DATA
                                reqMsg[1] = streamId
                                reqMsg[2] = uint8(numElements)
                                //binary.BigEndian.PutUint32(reqMsg[2:], numElements)
                                m.send(reqMsg[:])
                        }

                        producer.upstreamCancel = func() {
                                var reqMsg [2]byte
                                reqMsg[0] = MESSAGE_TYPE_CANCEL_STREAM
                                reqMsg[1] = streamId
                                m.send(reqMsg[:])
                        }
                        m.receiveStreams[streamId] = &producer
                        m.conduitCallback(&producer, message[2:])
                case MESSAGE_TYPE_STREAM_DATA:
                        //log.Printf("Data for stream: %d\n", streamId)
                        stream := m.receiveStreams[streamId]
                        stream.dataCallback(message[2:])
                case MESSAGE_TYPE_STREAM_END:
                        log.Printf("End stream: %d\n", streamId)
                        stream := m.receiveStreams[streamId]
                        stream.endCallback()
                case MESSAGE_TYPE_CANCEL_STREAM:
                        log.Printf("Terminate stream: %d\n", streamId)
                case MESSAGE_TYPE_STREAM_REQUEST_DATA:
                        log.Printf("Data requested for stream: %d\n", streamId)
                default:
                        log.Printf("Unsupported message type: %d\n", messageType)
                }
        }
}

func (m *Multiplexer) SetSendHandler(callback func([]byte)) {
        m.send = callback
}

func (m *Multiplexer) OnConduit(callback func(producer omnicore.Producer, metadata []byte)) {
        m.conduitCallback = callback
}


type ReceiveStream struct {
        upstreamRequest func(uint32)
        dataCallback func([]byte)
        endCallback func()
        upstreamCancel func()
        cancelCallback func()
}

func (s *ReceiveStream) Request(numElements uint32) {
        s.upstreamRequest(numElements)
}

func (s *ReceiveStream) OnData(callback func([]byte)) {
        s.dataCallback = callback
}

func (s *ReceiveStream) OnEnd(callback func()) {
        s.endCallback = callback
}

func (s *ReceiveStream) Pipe(consumer omnicore.Consumer) {

        s.OnData(func(data []byte) {
                consumer.Write(data)
        })

        // TODO: consider this interface. If there's already an onEnd listener
        // on the producer it won't get the notification
        s.OnEnd(func() {
                consumer.End()
        })

        s.OnCancel(func() {
                consumer.Cancel()
        })

        consumer.OnRequest(func(numElements uint32) {
                s.Request(numElements)
        })

        consumer.OnCancel(func() {
                s.upstreamCancel()
        })
}

func (s *ReceiveStream) Cancel() {
        s.upstreamCancel()
        s.cancelCallback()
}

func (s *ReceiveStream) OnCancel(callback func()) {
        s.cancelCallback = callback
}
