import Step from '../ATP/Step'
import query from './query'

export default {
  startStep,
  updateStatus,
  complete,
  getRecentPerformance,
}

async function startStep(stepId, stepType, transactionId, parentId, sequence, jsonDefinition) {
  // console.log(`dbStep.startStep(${stepId}, ${stepType}, ${transactionId}, ${parentId})`, jsonDefinition)
  // const json = JSON.stringify(definition, '', 2)

  const sql = `INSERT INTO atp_step_instance (step_id, step_type, transaction_id, parent_id, sequence, definition) VALUES (?, ?, ?, ?, ?, ?)`
  const params = [ stepId, stepType, transactionId, parentId, sequence, jsonDefinition ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
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
  let sql = `UPDATE atp_step_instance SET status=?, status_time=NOW(3), progress, percentage_complete`
  let params = [ status, progress, percentage ]
  if (status === Step.COMPLETED) {
    sql += `, status_time=NOW(3)`
  }
  sql += ` WHERE step_id=?`
  params.push(stepId)
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
}

/**
 *
 * @param {String} stepId
 * @param {String} response
 */
async function complete(stepId, response) {
  // console.log(`dbStep.complete(${stepId}, ${JSON.stringify(response, '', 0)})`.dim)
  let sql = `UPDATE atp_step_instance SET status=?, status_time=NOW(3), completion_time=NOW(3), progress=?, percentage_complete=?, response=? WHERE step_id=?`
  let params = [ Step.COMPLETED, Step.COMPLETED, 100, response, stepId ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
}

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


// SELECT step_id, TIMESTAMPDIFF(MICROSECOND, start_time, completion_time), start_time, completion_time FROM `atp_step_instance` WHERE step_type = 'pipeline'

// SELECT COUNT(status), AVG(TIMESTAMPDIFF(MICROSECOND, start_time, completion_time)) FROM `atp_step_instance` WHERE step_type = 'pipeline' AND status = 'completed'


// SELECT COUNT(status), AVG(TIMESTAMPDIFF(MICROSECOND, start_time, completion_time)) FROM `atp_step_instance` WHERE step_type = 'pipeline' AND status = 'completed' AND completion_time > (NOW() - INTERVAL 1 MINUTE)

