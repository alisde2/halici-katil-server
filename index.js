// Halıcı Katil — çok oyunculu oda sunucusu. Oyuncular bir oda koduyla bağlanır; sunucu her
// oyuncunun konum/karakter bilgisini aynı odadaki diğer oyunculara iletir (veri saklamaz).
import http from 'node:http'
import { randomUUID } from 'node:crypto'
import { WebSocketServer } from 'ws'

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' })
  res.end(`Halıcı Katil sunucusu çalışıyor · ${wss.clients.size} oyuncu`)
})
const wss = new WebSocketServer({ server, maxPayload: 65536 })
const rooms = new Map() // oda kodu -> Set<socket>

function broadcast(room, sender, message) {
  for (const client of rooms.get(room) ?? []) {
    if (client !== sender && client.readyState === 1) client.send(message)
  }
}

wss.on('connection', socket => {
  socket.id = randomUUID().slice(0, 8)
  socket.room = null
  socket.alive = true
  socket.on('pong', () => { socket.alive = true })
  socket.on('message', raw => {
    let message
    try { message = JSON.parse(raw) } catch { return }
    if (message.t === 'join' && typeof message.room === 'string' && !socket.room) {
      const room = message.room.trim().toUpperCase().slice(0, 12) || 'GENEL'
      const members = rooms.get(room) ?? new Set()
      if (members.size >= 4) return socket.close(4000, 'Oda dolu')
      members.add(socket)
      rooms.set(room, members)
      socket.room = room
      socket.send(JSON.stringify({ t: 'welcome', id: socket.id }))
    } else if (message.t === 'state' && socket.room && message.state) {
      broadcast(socket.room, socket, JSON.stringify({ t: 'state', id: socket.id, state: message.state }))
    } else if ((message.t === 'world' || message.t === 'event') && socket.room) {
      // Oda sahibinin düşman konumları ve ortak olaylar (kapı, eşya) herkese.
      broadcast(socket.room, socket, JSON.stringify({ ...message, id: socket.id }))
    } else if (message.t === 'signal' && socket.room && typeof message.to === 'string') {
      // Sesli sohbet bağlantı bilgisi sadece hedef oyuncuya.
      for (const client of rooms.get(socket.room) ?? []) {
        if (client.id === message.to && client.readyState === 1) client.send(JSON.stringify({ t: 'signal', from: socket.id, data: message.data }))
      }
    }
  })
  socket.on('close', () => {
    if (!socket.room) return
    const members = rooms.get(socket.room)
    members?.delete(socket)
    broadcast(socket.room, socket, JSON.stringify({ t: 'leave', id: socket.id }))
    if (members && members.size === 0) rooms.delete(socket.room)
  })
})

// Kopan bağlantıları temizle.
setInterval(() => {
  for (const socket of wss.clients) {
    if (!socket.alive) { socket.terminate(); continue }
    socket.alive = false
    socket.ping()
  }
}, 30000)

const port = Number(process.env.PORT) || 10000
server.listen(port, () => console.log(`Halıcı Katil sunucusu ${port} portunda`))
