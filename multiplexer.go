package omniconc

import (
        "log"
        "net/http"
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
        streamCallback func(omnicore.Producer)
}

func (m *Multiplexer) SendControlMessage(message []byte) {
        var reqMsg [2]byte
        reqMsg[0] = MESSAGE_TYPE_CONTROL_MESSAGE
        reqMsg[1] = 22
        m.send(reqMsg[:])
}

func (m *Multiplexer) HandleMessage(message []byte) {
        messageType := message[0]

        if messageType == MESSAGE_TYPE_CONTROL_MESSAGE {
                log.Println("Control message")
        } else {
                streamId := message[1]
                switch messageType {
                case MESSAGE_TYPE_CREATE_RECEIVE_STREAM:
                        log.Printf("Create stream: %d\n", streamId)
                        stream := ReceiveStream{}
                        stream.request = func(numElements uint8) {
                                var reqMsg [6]byte
                                reqMsg[0] = MESSAGE_TYPE_STREAM_REQUEST_DATA
                                reqMsg[1] = streamId
                                reqMsg[2] = numElements
                                //binary.BigEndian.PutUint32(reqMsg[2:], numElements)
                                m.send(reqMsg[:])
                        }
                        m.receiveStreams[streamId] = &stream
                        m.streamCallback(&stream)
                case MESSAGE_TYPE_STREAM_DATA:
                        //log.Printf("Data for stream: %d\n", streamId)
                        stream := m.receiveStreams[streamId]
                        stream.dataCallback(message[2:])
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

func (m *Multiplexer) OnStream(callback func(omnicore.Producer)) {
        m.streamCallback = callback
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

type WebSocketMuxAcceptor struct {
        httpHandler func(http.ResponseWriter, *http.Request)
}

func (m *WebSocketMuxAcceptor) GetHttpHandler() func(http.ResponseWriter, *http.Request) {
        return m.httpHandler
}
