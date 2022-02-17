/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';

export async function handleOrphanQueuesV1(req, res, next) {
  console.log(`handleOrphanQueuesV1()`)

  // console.log(`req.params=`, req.params)

  const nodeGroup = req.params.nodeGroup
  const nodeId = req.params.nodeId
  console.log(`nodeGroup=`, nodeGroup)
  console.log(`nodeId=`, nodeId)

  // const numMoved = 0
  const numMoved = await schedulerForThisNode.handleOrphanQueues(nodeGroup, nodeId)

  res.send({ moved: numMoved })
  return next()
}
