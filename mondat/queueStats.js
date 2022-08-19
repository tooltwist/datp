/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
// import TransactionCache from '../ATP/Scheduler2/TransactionCache';
// import LongPoll from '../ATP/Scheduler2/LongPoll'
import Scheduler2 from '../ATP/Scheduler2/Scheduler2'
import { getNodeGroups } from '../database/dbNodeGroup';

const VERBOSE = 0

/**
 * Get details of all the queues, nodes and node groups.
 * @param {Request} req 
 * @param {Response} res 
 * @param {*} next 
 */
export async function mondatRoute_getQueueStatsV1(req, res, next) {
  if (VERBOSE) console.log(`-----------------`)
  if (VERBOSE) console.log(`mondatRoute_getQueueStatsV1()`)

  const stats = { }
  const getGroup = (nodeGroup) => {
    let grp = stats[nodeGroup]
    if (!grp) {
      grp = { nodeGroup, queueLength: 0, regularQueueLength: 0, expressQueueLength: 0, nodes: { }, orphanNodes: { } }
      stats[nodeGroup] = grp
    }
    return grp
  }
  // const intArray = (size) => { const arr = [ ]; for (let i = 0; i < size; i++) arr.push(0); return arr }
  const getNode = async (group, nodeId, shouldBeKnown=false) => {
    // console.log(`        getNode(${group}, ${nodeId}, shouldBeKnown=${shouldBeKnown})`)

    let node = group.nodes[nodeId]
    if (node) {
      // console.log(`        - found node in main list`, node)
      return node
    }

    // The node was not found in the main list.
    // Perhaps it is already registered as an orphan?
    node = group.orphanNodes[nodeId]
    if (node) {
      // console.log(`        - found node in orphan list`, node)
      return node
    }

    // Let's get the node details from REDIS
    if (!shouldBeKnown) {
      node = await schedulerForThisNode.getNodeDetailsFromREDIS(group.nodeGroup, nodeId)
      if (node) {
        group.nodes[nodeId] = node
        return node
      }
    }


    /*
     *  We are getting details for a node that is not running. We know this because
     *  its keepalive has not saved details to REDIS recently, so they've been removed.
     *    i) It is in the node groups list, which hasn't removed the node yet.
     *    ii) A queue refers to the node.
     *  In either case we need to consider this an orphan node.
     */
    // console.log(`------------------------> Creating fake details for orphan ${nodeId}`)

    // Create a fake new node for an orphin
    node = {
      nodeId,
      queueLength: 0,
      regularQueueLength: 0,
      expressQueueLength: 0,
      workers: {total: 0, running: 0, waiting: 0, shuttingDown: 0, standby: 0, required: 0},
      stats: {
        // transactionsInPastMinute: intArray(60),
        // transactionsInPastHour: intArray(60),
        // transactionsOutPastMinute: intArray(60),
        // transactionsOutPastHour: intArray(60),
        // stepsPastMinute: intArray(60),
        // stepsPastHour: intArray(60),
        // enqueuePastMinute: intArray(60),
        // enqueuePastHour: intArray(60),
        // dequeuePastMinute: intArray(60),
        // dequeuePastHour: intArray(60),
      },
      throughput: {
        txInSec: 0,
        txOutSec: 0
      },
      transactionsInCache: 0,
      outstandingLongPolls: 0,
      stepTypes: [ ],
    }
    console.log(`FOUND AN ORPHAN NODE [${nodeId}] IN GROUP ${group.nodeGroup}`)
    group.orphanNodes[nodeId] = node
    return node

  }//- getNode()

  // Get the groups from the database
  const groups = await getNodeGroups()
  // console.log(`groups=`, groups)
  // Index them by nodeGroup name
  // const index =  { }
  for (const group of groups) {
    // Check it's in the list
    getGroup(group.nodeGroup)
  }

  // Get a list of currently active nodes (grouped by nodeGroup)
  const withSteptypes = false
  const activeNodeGroups = await schedulerForThisNode.getDetailsOfActiveNodesfromREDIS(withSteptypes)
  // console.log(`activeNodeGroups=`, activeNodeGroups)
  for (const activeGroup of activeNodeGroups) {
    // console.log(`  activeGroup=`, activeGroup)
    // Add the group and node to our lists
    const group = getGroup(activeGroup.nodeGroup)
    group.queueLength = 0
    group.regularQueueLength = 0
    group.expressQueueLength = 0
    // console.log(`    group=`, group)
    for (const nodeId of activeGroup.nodes) {
      // console.log(`    nodeId=`, nodeId)
      const node = await getNode(group, nodeId)
      node.queueLength = 0
      node.regularQueueLength = 0
      node.expressQueueLength = 0
    }
    // console.log(`^^^`)
  }

  /*
   *  Get the queue sizes from REDIS.
   *  We've already loaded all the currently running nodes, so any
   *  queue for a node that is unknown must be evidence of a node
   *  that has died, and left behind an orphan queue.
   */
  const queues = await schedulerForThisNode.queueLengths() // [ { nodeGroup, nodeId?, queueLength} ]
  const orphanIfNotAlreadyFound = true
  for (const queue of queues) {
    // console.log(`queue=`, queue)

    const parts = queue.name.split(':')
    if (parts.length > 1) {
      const type = parts[0]
      const nodeGroup = parts[1]
      const nodeId = (parts.length >= 3) ? parts[2] : null
      // console.log(`  nodeId=`, nodeId)
      const grp = getGroup(nodeGroup)

      switch (type) {
        case Scheduler2.GROUP_QUEUE_PREFIX:
          // Queue is for the group
          // console.log(`Group queue length ${queue.length} for ${nodeGroup}`)
          grp.queueLength += queue.length
          grp.regularQueueLength += queue.length
          break
        case Scheduler2.GROUP_EXPRESS_QUEUE_PREFIX:
          // Queue is for the group
          // console.log(`Group queue length ${queue.length} for ${nodeGroup}`)
          grp.queueLength += queue.length
          grp.expressQueueLength += queue.length
          break
        case Scheduler2.REGULAR_QUEUE_PREFIX:
          {
            // Regular node queue
            const node = await getNode(grp, nodeId, orphanIfNotAlreadyFound)
            if (node) {
              // console.log(`Regular queue length ${queue.length} for ${nodeId}`)
              node.queueLength += queue.length
              node.regularQueueLength += queue.length
            }
          }
          break
        case Scheduler2.EXPRESS_QUEUE_PREFIX:
          {
            // Express queue for the node
            const node = await getNode(grp, nodeId, orphanIfNotAlreadyFound)
            if (node) {
              // console.log(`Express queue length ${queue.length} for ${nodeId}`)
              node.queueLength += queue.length
              node.expressQueueLength += queue.length
            }
          }
          break

        default:
          console.trace(`Internal Error: unknown node type [${type}]`)
      }
    }
  }

  // for (const node of Object.values(stats.master.nodes)) {
  //   console.log(`=>`, JSON.stringify(node.stats.transactionsInPastMinute, '', 2))
  // }
  // console.log(`stats=`, JSON.stringify(stats, '', 2))
  // console.log(`RETURNING STATS`)
  // console.log(`YARP 999 stats=`, stats)
  res.send(stats)
  next();
}
