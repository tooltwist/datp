import query from './query'

export default {
  log,
  logbookEntries,
}

/**
 *
 * @param {String} msg
 */
async function log(transactionId, stepId, sequence, msg) {
  console.log(`log(${transactionId}, ${stepId}, ${sequence}, ${msg})`)

  if (typeof(json) !== 'string') {
    throw new Error('log() requires string value for msg parameter')
  }
  const sql = `INSERT INTO atp_logbook (transaction_id, step_id, sequence, message) VALUES (?, ?, ?, ?)`
  const params = [ transactionId, stepId, sequence, msg ]
  console.log(`sql=`, sql)
  console.log(`params=`, params)
  const result = await query(sql, params)
  console.log(`result=`, result)
}

async function logbookEntries(transactionId) {
  console.log(`logbookEntries(${transactionId})`)
  const sql = `SELECT step_id AS stepId, sequence, message FROM atp_logbook WHERE transaction_id=? ORDER BY sequence`
  const params = [ trasactionId ]
  console.log(`sql=`, sql)
  console.log(`params=`, params)
  const result = await query(sql, params)
  console.log(`result=`, result)
  return result
}