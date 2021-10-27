/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'

export default {
  getStepInstanceDetails
}

export async function getStepInstanceDetails(stepId) {
  // console.log(`getStepInstanceDetails(${stepId})`)

  const sql = `SELECT
      step_id AS stepId,
      step_type AS stepType,
      status,
      transaction_id AS transactionId,
      parent_id AS parentId,
      sequence,
      definition,
      progress,
      percentage_complete AS percentageComplete,
      response,
      start_time AS startTime,
      status_time AS statusTime,
      completion_time AS completionTime
  FROM atp_step_instance WHERE step_id=?`

  const params = [ stepId ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result.length > 0 ? result[0] : null
}
