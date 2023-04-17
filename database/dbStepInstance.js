/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
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
      completion_time AS completionTime,
      last_updated AS lastUpdated
  FROM atp_step_instance WHERE step_id=?`

  const params = [ stepId ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result.length > 0 ? result[0] : null
}
