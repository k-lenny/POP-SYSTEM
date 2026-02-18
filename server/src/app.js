require('dotenv').config()
const express = require('express')
const cors = require('cors')

const app = express()
const PORT = process.env.PORT || 4000

// Middleware
app.use(cors({ origin: 'http://localhost:5173' })) // Vite default port
app.use(express.json())

// Health check route — test this first in Postman
app.get('/api/health', (req, res) => {
  res.json({
    status: 'ok',
    message: 'Trading server is running',
    timestamp: new Date().toISOString(),
  })
})

// Start server
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`)
})