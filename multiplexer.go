package omniconc

import (
        "log"
        "github.com/anderspitman/omnistreams-core-go"
)

const MESSAGE_TYPE_CREATE_RECEIVE_STREAM = 0
const MESSAGE_TYPE_STREAM_DATA = 1
const MESSAGE_TYPE_STREAM_END = 2
const MESSAGE_TYPE_TERMINATE_SEND_STREAM = 3
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
                        producer.request = func(numElements uint8) {
                                var reqMsg [6]byte
                                reqMsg[0] = MESSAGE_TYPE_STREAM_REQUEST_DATA
                                reqMsg[1] = streamId
                                reqMsg[2] = numElements
                                //binary.BigEndian.PutUint32(reqMsg[2:], numElements)
                                m.send(reqMsg[:])
                        }
                        m.receiveStreams[streamId] = &producer
                        m.conduitCallback(&producer, message[2:])
                case MESSAGE_TYPE_STREAM_DATA:
                        //log.Printf("Data for stream: %d\n", streamId)
                        stream := m.receiveStreams[streamId]
                        // TODO: reconsider if this is safe to call in a goroutine
                        go stream.dataCallback(message[2:])
                case MESSAGE_TYPE_STREAM_END:
                        log.Printf("End stream: %d\n", streamId)
                        stream := m.receiveStreams[streamId]
                        stream.endCallback()
                case MESSAGE_TYPE_TERMINATE_SEND_STREAM:
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
        request func(uint8)
        dataCallback func([]byte)
        endCallback func()
}

func (s *ReceiveStream) OnData(callback func([]byte)) {
        s.dataCallback = callback
}

func (s *ReceiveStream) OnEnd(callback func()) {
        s.endCallback = callback
}

func (s *ReceiveStream) Request(numElements uint8) {
        s.request(numElements)
}
