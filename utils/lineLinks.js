function firstEnv(...names) {
  for (const name of names) {
    const value = String(process.env[name] || '').trim()
    if (value) return value
  }
  return ''
}

function normalizePath(path = '/') {
  const value = String(path || '/').trim()
  if (/^https?:\/\//i.test(value)) return value
  return value.startsWith('/') ? value : `/${value}`
}

function publicFrontendUrl() {
  return firstEnv('VITE_FRONTEND_URL', 'FRONTEND_URL').replace(/\/+$/, '')
}

function lineLiffId() {
  return firstEnv('VITE_LIFF_ID', 'LIFF_ID')
}

function liffUrl(path = '/') {
  const normalizedPath = normalizePath(path)
  if (/^https?:\/\//i.test(normalizedPath)) return normalizedPath

  const liffId = lineLiffId()
  if (liffId) {
    const liffPath = normalizedPath.startsWith('/line')
      ? normalizedPath.slice(5)
      : normalizedPath
    return `https://liff.line.me/${liffId}${liffPath || '/'}`
  }

  const base = publicFrontendUrl()
  return base ? `${base}${normalizedPath}` : normalizedPath
}

function allowedFrontendOrigins() {
  return [process.env.VITE_FRONTEND_URL, process.env.FRONTEND_URL]
    .filter(Boolean)
    .join(',')
    .split(',')
    .map(s => s.trim().replace(/\/+$/, ''))
    .filter(Boolean)
}

module.exports = {
  allowedFrontendOrigins,
  liffUrl,
  lineLiffId,
  publicFrontendUrl,
}
