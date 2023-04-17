/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { getStepInstanceDetails } from "../database/dbStepInstance"

export async function mondatRoute_getStepInstanceDetailsV1(req, res, next) {
  // console.log(`mondatRoute_getStepInstanceDetailsV1()`)

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
