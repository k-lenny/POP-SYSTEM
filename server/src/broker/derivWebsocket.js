// server/src/broker/derivWebSocket.js
require('dotenv').config()
const WebSocket = require('ws')

class DerivWebSocket {
  constructor() {
    this.ws = null
    this.isConnected = false
    this.messageHandlers = []
    this.reconnectHandlers = []
    this.reconnectInterval = 5000
  }

  connect() {
    const appId = process.env.DERIV_APP_ID

    const url = `wss://ws.binaryws.com/websockets/v3?app_id=${appId}&l=EN&brand=deriv`

    console.log('[Deriv] Connecting to:', url)

    this.ws = new WebSocket(url)

    this.ws.on('open', () => {
      console.log('[Deriv] Connected successfully')
      this.isConnected = true

      if (process.env.DERIV_API_TOKEN) {
        console.log('[Deriv] Authorizing...')
        this.send({ authorize: process.env.DERIV_API_TOKEN })
      } else {
        console.log('[Deriv] No token — connected for market data only')
        this.messageHandlers.forEach(handler =>
          handler({ msg_type: 'open' })
        )
      }
    })

    this.ws.on('message', (rawData) => {
      try {
        const data = JSON.parse(rawData)

        if (data.error) {
          console.error('[Deriv] API Error:', data.error.message)
          return
        }

        this.messageHandlers.forEach(handler => handler(data))

      } catch (error) {
        console.error('[Deriv] Failed to parse message:', error.message)
      }
    })

    this.ws.on('error', (error) => {
      console.error('[Deriv] WebSocket error:', error.message)
    })

    this.ws.on('close', () => {
      console.log('[Deriv] Connection closed — reconnecting in 5 seconds...')
      this.isConnected = false

      // Notify signalEngine so it can reset and resubscribe
      this.reconnectHandlers.forEach(handler => handler())

      setTimeout(() => this.connect(), this.reconnectInterval)
    })
  }

  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data))
    } else {
      console.error('[Deriv] Cannot send — not connected')
    }
  }

  onMessage(handler) {
    this.messageHandlers.push(handler)
  }

  onReconnect(handler) {
    this.reconnectHandlers.push(handler)
  }

  disconnect() {
    if (this.ws) {
      this.ws.close()
    }
  }
}

module.exports = new DerivWebSocket()