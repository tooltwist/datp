/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
import { getNodeGroups } from '../database/dbNodeGroup';

export async function route_activeNodesV1(req, res, next) {
  // console.log(`route_activeNodesV1()`)
  try {
    let withStepTypes = false
    if (req.query.stepTypes) {
      withStepTypes = true
    }

    // Get the list of active nodes.
    const nodeList = await schedulerForThisNode.getDetailsOfActiveNodes(withStepTypes)

    // If any defined groups are missing, add them even though the group is not active.
    const existingNodeGroups = new Set()
    nodeList.forEach(group => existingNodeGroups.add(group.nodeGroup))
    const groups = await getNodeGroups()
    for (const group of groups) {
      // If this group is not in our index, we need to add an empty group to our result.
      if (!existingNodeGroups.has(group.nodeGroup)) {
        // console.log(`append an empty node group ${group.nodeGroup}`)
        nodeList.push({
          nodeGroup: group.nodeGroup,
          nodes: [ ],
          stepTypes: [ ],
          warnings: [ ]
        })
      }
    }

    res.send(nodeList)
    return next();
  } catch (e) {
    console.log(`Error in route_activeNodesV1():`, e)
  }
}
