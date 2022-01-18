/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
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
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
}

async function logbookEntries(transactionId) {
  console.log(`logbookEntries(${transactionId})`)
  const sql = `SELECT step_id AS stepId, sequence, message FROM atp_logbook WHERE transaction_id=? ORDER BY sequence`
  const params = [ trasactionId ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result
}