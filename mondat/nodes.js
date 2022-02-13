/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';

export async function routeListNodesV1(req, res, next) {
  // console.log(`routeListNodesV1()`)
  try {
    const nodeList = await schedulerForThisNode.getNodeIds()
    res.send(nodeList)
    return next();
  } catch (e) {
    console.log(`Error in routeListNodesV1():`, e)
  }
}
