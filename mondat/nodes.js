/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
import { getNodeGroups } from '../database/dbNodeGroup';

export async function mondatRoute_activeNodesV1(req, res, next) {
  // console.log(`mondatRoute_activeNodesV1()`)
  try {
    let withStepTypes = false
    if (req.query.stepTypes) {
      withStepTypes = true
    }

    // Get the list of active nodes.
    const nodeList = await schedulerForThisNode.getDetailsOfActiveNodesfromREDIS(withStepTypes)

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
    console.log(`Error in mondatRoute_activeNodesV1():`, e)
  }
}
