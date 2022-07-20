/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { saveNodeGroup } from "../database/dbNodeGroup"

export async function setNumWorkersV1(req, res, next) {
  // console.log(`setRequiredWorkersV1()`)
  const nodeGroup = req.params.nodeGroup
  const numWorkers = req.body.numWorkers
  await saveNodeGroup({ nodeGroup, numWorkers })

  res.send({ status: 'ok' })
  return next()
}
