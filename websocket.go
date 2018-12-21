package omniconc

import (
        "log"
        "net/http"
        "github.com/anderspitman/omnistreams-core-go"
        "github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{}

func CreateWebSocketMuxAcceptor() WebSocketMuxAcceptor {
        upgrader.CheckOrigin = func(r *http.Request) bool {
                return true
        }

        return WebSocketMuxAcceptor {
                httpHandler: wsHttpHandler,
        }
}

func wsHttpHandler(w http.ResponseWriter, r *http.Request) {

        wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
        defer wsConn.Close()

        writeChan := make(chan []byte)

        mux := CreateMultiplexer()

        mux.SetSendHandler(func(message []byte) {
                writeChan <- message
        })

        mux.OnStream(func(stream omnicore.Producer) {

                log.Println("Got a streamy")

                stream.OnData(func(data []byte) {
                        log.Println(len(data))
                        stream.Request(1)
                })

                stream.OnEnd(func() {
                        log.Println("end streamy")
                        mux.SendControlMessage([]byte("hi there"))
                })

                stream.Request(10)
        })

        writeMessages := func() {
                for message := range writeChan {
                        if err := wsConn.WriteMessage(websocket.BinaryMessage, message); err != nil {
                                log.Println(err)
                                return
                        }
                }
        }
        go writeMessages()

        for {
                _, p, err := wsConn.ReadMessage()
                if err != nil {
                        log.Println(err)
                        return
                }

                mux.HandleMessage(p)
        }
}
