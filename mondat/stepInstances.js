/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { getStepInstanceDetails } from "../database/dbStepInstance"

export async function getStepInstanceDetailsV1(req, res, next) {
  // console.log(`getStepInstanceDetailsV1()`)

  const stepId = req.params.stepId
  const details = await getStepInstanceDetails(stepId)

  //ZZZZ Get log entries

  //ZZZZ Get artifacts
  const reply = {
    details
  }
  res.send(reply)
  return next()
}
