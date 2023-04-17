/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import Step, { STEP_RUNNING } from '../ATP/Step'
import dbupdate from './dbupdate'
import query from './query'

export default {
  startStep,
  updateStatus,
  // saveExitStatus,
  getRecentPerformance,
}

async function startStep(stepId, stepType, transactionId, parentId, sequence, jsonDefinition) {
  // console.log(`dbStep.startStep(${stepId}, ${stepType}, ${transactionId}, ${parentId})`, jsonDefinition)
  // const json = JSON.stringify(definition, '', 2)

  const sql = `INSERT INTO atp_step_instance (step_id, step_type, transaction_id, parent_id, sequence, definition) VALUES (?, ?, ?, ?, ?, ?)`
  const params = [ stepId, stepType, transactionId, parentId, sequence, jsonDefinition ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  // console.log(`result=`, result)
}

/**
 *
 * @param {String} stepId
 * @param {String} status
 * @param {String} progress
 * @param {Number} percentage
 */
async function updateStatus(stepId, status, progress, percentage) {
  // console.log(`\n\n*-*-*-*-*-*-*dbStep.updateStatus(${stepId}, ${status}, ${progress}, ${percentage})\n`)
  throw new Error(`temporarily Deprecated.`)

  // Check the status has not already been set to completed
  let sql1 = `SELECT status FROM atp_step_instance WHERE step_id=?`
  let params1 = [ stepId ]
  const result1 = await query(sql1, params1)
  console.log(`Existing step status =`, result1)


  // Update the status now
  let sql = `UPDATE atp_step_instance SET status=?, status_time=NOW(3), progress=?, percentage_complete=?`
  let params = [ status, progress, percentage ]
  if (status === STEP_COMPLETED) {
    sql += `, status_time=NOW(3)`
  }
  sql += ` WHERE step_id=?`
  params.push(stepId)
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  // console.log(`result=`, result)
}

/**
 *
 * @param {String} stepId
 * @param {String} response
 */
// async function saveExitStatus(stepId, status, response) {
//   // console.log(`\n\n*-*-*-*-*-*-*dbStep.saveExitStatus(${stepId}, ${status})\n`)

//   // Check the status has not already been set to completed
//   let sql1 = `SELECT status FROM atp_step_instance WHERE step_id=?`
//   let params1 = [ stepId ]
//   const result1 = await query(sql1, params1)
//   if (result1.length < 1) {
//     const msg = `Internal error: step ${stepId} not in the database`
//     console.trace(msg)
//     throw new Error(msg)
//   }
//   if (result1[0].status !== STEP_RUNNING) {
//      const msg = `Trying to set exist status of non-running step ${stepId}.`
//      console.trace(msg)
//      throw new Error(msg)
//   }
//   // console.log(`Existing step status =`, result1)
//   console.log(`   ====> Previous status=${result1[0].status}. Setting to ${status}.`)


//   // Update the status now
//   console.log(`dbStep.saveExitStatus(${stepId}, ${status}, ${JSON.stringify(response, '', 0)})`.dim)
//   let sql = `UPDATE atp_step_instance SET status=?, status_time=NOW(3), completion_time=NOW(3), progress=?, percentage_complete=?, response=? WHERE step_id=?`
//   let params = [ status, status, 100, response, stepId ]
//   // console.log(`sql=`, sql)
//   // console.log(`params=`, params)
//   const result = await dbupdat(sql, params)
//   // console.log(`result=`, result)
// }

async function getRecentPerformance(seconds) {
  // console.log(`getRecentPerformance(${seconds})`)
  const sql = `
    SELECT step_type AS stepType, COUNT(step_type) AS count, (COUNT(step_type) * 60 / ?) AS perMinute, ROUND(AVG(TIMESTAMPDIFF(MICROSECOND, start_time, completion_time)) / 1000) AS ms
    FROM atp_step_instance
    WHERE status = 'completed'
    AND completion_time > (NOW() - INTERVAL ? SECOND)
    GROUP BY step_type`


  // let sql = `UPDATE atp_step_instance SET status=?, status_time=NOW(3), completion_time=NOW(3), progress=?, percentage_complete=?, response=? WHERE step_id=?`
  let params = [ seconds, seconds ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result
}


// SELECT step_id, TIMESTAMPDIFF(MICROSECOND, start_time, completion_time), start_time, completion_time FROM `atp_step_instance` WHERE step_type = 'hidden/pipeline'

// SELECT COUNT(status), AVG(TIMESTAMPDIFF(MICROSECOND, start_time, completion_time)) FROM `atp_step_instance` WHERE step_type = 'hidden/pipeline' AND status = 'completed'

// SELECT COUNT(status), AVG(TIMESTAMPDIFF(MICROSECOND, start_time, completion_time)) FROM `atp_step_instance` WHERE step_type = 'hidden/pipeline' AND status = 'completed' AND completion_time > (NOW() - INTERVAL 1 MINUTE)
