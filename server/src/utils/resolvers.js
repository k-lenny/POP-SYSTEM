// server/src/utils/resolvers.js
const { volatilitySymbols, timeframes } = require('../signals/signalEngine')

// ── Build lookup sets from signalEngine — single source of truth ──
const validSymbolCodes   = new Set(Object.values(volatilitySymbols))
const validGranularities = new Set(Object.values(timeframes))

// ── Resolve symbol — accepts code 'R_10' or name 'Volatility 10' ──
const resolveSymbol = (input) => {
  if (!input) return null
  // Direct code match e.g R_10
  if (validSymbolCodes.has(input)) return input
  // Name match e.g 'Volatility 10'
  const code = volatilitySymbols[input]
  if (code) return code
  return null
}

// ── Resolve granularity — accepts number 3600 or label '1 hour' ──
const resolveGranularity = (input) => {
  if (!input) return null
  const asInt = parseInt(input)
  if (!isNaN(asInt) && validGranularities.has(asInt)) return asInt
  // Label match e.g '1 hour'
  const fromLabel = timeframes[input]
  if (fromLabel) return fromLabel
  return null
}

// ── Consistent error response ──
const sendError = (res, status, message, details = null) => {
  const body = { success: false, error: message }
  if (details) body.details = details
  return res.status(status).json(body)
}

// ── Consistent success response ──
const sendSuccess = (res, data) => {
  return res.status(200).json({ success: true, ...data })
}

// ── Request logger ──
const logRequest = (req) => {
  console.log(`[Routes] ${req.method} ${req.originalUrl} — ${new Date().toISOString()}`)
}

// ── Valid options helpers — used in error detail responses ──
const getValidSymbols = () =>
  Object.entries(volatilitySymbols).map(([name, code]) => ({ name, code }))

const getValidGranularities = () =>
  Object.entries(timeframes).map(([label, value]) => ({ label, value }))

module.exports = {
  resolveSymbol,
  resolveGranularity,
  sendError,
  sendSuccess,
  logRequest,
  getValidSymbols,
  getValidGranularities,
}