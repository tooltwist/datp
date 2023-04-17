/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { saveNodeGroup } from "../database/dbNodeGroup"

export async function mondatRoute_setEventloopWorkersV1(req, res, next) {
  // console.log(`setRequiredWorkersV1()`)
  const nodeGroup = req.params.nodeGroup
  const eventloopWorkers = req.body.eventloopWorkers
  await saveNodeGroup({ nodeGroup, eventloopWorkers })

  res.send({ status: 'ok' })
  return next()
}
