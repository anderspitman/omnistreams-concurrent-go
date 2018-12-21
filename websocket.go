package omniconc

import (
        "log"
        "net/http"
        "github.com/gorilla/websocket"
)


type WebSocketMuxAcceptor struct {
        httpHandler func(http.ResponseWriter, *http.Request)
        muxCallback func(mux *Multiplexer)
}

func (m *WebSocketMuxAcceptor) GetHttpHandler() func(http.ResponseWriter, *http.Request) {
        return m.httpHandler
}

func (m *WebSocketMuxAcceptor) OnMux(callback func(mux *Multiplexer)) {
        m.muxCallback = callback
}


var upgrader = websocket.Upgrader{}

func CreateWebSocketMuxAcceptor() *WebSocketMuxAcceptor {
        upgrader.CheckOrigin = func(r *http.Request) bool {
                return true
        }

        muxChan := make(chan *Multiplexer)

        var wsHttpHandler = func(w http.ResponseWriter, r *http.Request) {

                wsConn, err := upgrader.Upgrade(w, r, nil)
                if err != nil {
                        log.Print("upgrade:", err)
                        return
                }
                defer wsConn.Close()

                writeChan := make(chan []byte)

                mux := CreateMultiplexer()
                muxChan <- &mux

                mux.SetSendHandler(func(message []byte) {
                        writeChan <- message
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

        muxAcceptor := WebSocketMuxAcceptor {
                httpHandler: wsHttpHandler,
        }

        passMuxes := func() {
                for mux := range muxChan {
                        log.Println(muxAcceptor.muxCallback)
                        muxAcceptor.muxCallback(mux)
                }
        }
        go passMuxes()

        return &muxAcceptor
}


