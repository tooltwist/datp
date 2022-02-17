/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
import { getNodeGroups } from '../database/dbNodeGroup';

export async function routeListNodeGroupsV1(req, res, next) {
  // console.log(`routeListNodeGroupsV1()`)
  try {

    // Get the groups from the database
    const groups = await getNodeGroups()
    // console.log(`groups=`, groups)

    // Index them by nodeGroup name
    const index =  { }
    for (const group of groups) {
      group.activeNodes = 0
      index[group.nodeGroup] = group
    }

    // Get the details of active nodes from REDIS
    const nodeList = await schedulerForThisNode.getNodeIds()
    // console.log(`nodeList=`, nodeList)

    // Overlay the active node details onto the groups
    for (const activeGroup of nodeList) {
      const group = index[activeGroup.nodeGroup]
      if (group) {
        group.activeNodes = activeGroup.nodes.length
      } else {
        // This should rarely happen - we have an active node that
        // is not in the database. It must have been deleted recently.
        console.log(`DANGEROUS - RUNNING NODE WITH GROUP NOT DEFINED IN THE DATABASE (${activeGroup.nodeGroup})`)
      }
    }
    res.send(groups)
    return next();

  } catch (e) {
    console.log(`Error in routeListNodesV1():`, e)
  }
}
