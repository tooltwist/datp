/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
import TransactionCache from '../ATP/Scheduler2/TransactionCache';
import LongPoll from '../ATP/Scheduler2/LongPoll'
import Scheduler2 from '../ATP/Scheduler2/Scheduler2'

export async function getNodeStatsV1(req, res, next) {
  // console.log(`getNodeStatsV1()`)

  const stats = { }
  const getGroup = (nodeGroup) => {
    let grp = stats[nodeGroup]
    if (!grp) {
      grp = { nodeGroup, queueLength: 0, nodes: { } }
      stats[nodeGroup] = grp
    }
    return grp
  }
  const intArray = (size) => { const arr = [ ]; for (let i = 0; i < size; i++) arr.push(0); return arr }
  const getNode = (group, nodeId) => {
    let node = group.nodes[nodeId]
    if (!node) {
      node = {
        nodeId,
        queueLength: 0,
        regularQueueLength: 0,
        expressQueueLength: 0,
        workers: {total: 0, running: 0, waiting: 0, shuttingDown: 0, standby: 0},
        stats: {
          transactionsInPastMinute: intArray(60),
          transactionsInPastHour: intArray(60),
          transactionsOutPastMinute: intArray(60),
          transactionsOutPastHour: intArray(60),
          stepsPastMinute: intArray(60),
          stepsPastHour: intArray(60),
          enqueuePastMinute: intArray(60),
          enqueuePastHour: intArray(60),
          dequeuePastMinute: intArray(60),
          dequeuePastHour: intArray(60),
        }
      }
      group.nodes[nodeId] = node
    }
    return node
  }

  // Create the initial stats containing entries for all known nodes.
{
  const nodeGroup = 'master'
  const nodeId = schedulerForThisNode.getNodeId()

  const myStatus = await schedulerForThisNode.getStatus()
  // console.log(`myStatus=`, myStatus)
  const transactionsInCache = await TransactionCache.size()
  const outstandingLongPolls = await LongPoll.outstandingLongPolls()
  // console.log(`transactionsInCache=`, transactionsInCache)

  // console.log(`YARP 1:`, myStatus.stats.transactionsInPastMinute)

  const grp = getGroup(nodeGroup)
  grp.nodes[nodeId]

  if (nodeId) {
    const node = getNode(grp, nodeId)
    node.workers = myStatus.workers
    node.stats = myStatus.stats
    node.transactionsInCache = transactionsInCache
    node.outstandingLongPolls = outstandingLongPolls
    // console.log(`node=`, node)
    // console.log(`YARP 2:`, node.stats.transactionsInPastMinute)
  }
}


  // Add the REDIS queue sizes
  // const queue = await getQueueConnection()
  // console.log(`queue=`, queue)
  const queues = await schedulerForThisNode.queueLengths() // [ { nodeGroup, nodeId?, queueLength} ]
  for (const queue of queues) {
    // console.log(`queue=`, queue)

    const parts = queue.name.split(':')
    if (parts.length > 1) {
      const type = parts[0]
      const nodeGroup = parts[1]
      const nodeId = (parts.length > 2) ? parts[2] : null
      const grp = getGroup(nodeGroup)

      switch (type) {
        case Scheduler2.GROUP_QUEUE_PREFIX:
          // Queue is for the group
          grp.queueLength += queue.length
          break
        case Scheduler2.REGULAR_QUEUE_PREFIX:
          {
            // Regular node queue
            const node = getNode(grp, nodeId)
            node.queueLength += queue.length
            node.regularQueueLength = queue.length
          }
          break
        case Scheduler2.EXPRESS_QUEUE_PREFIX:
          {
            // Express queue for the node
            const node = getNode(grp, nodeId)
            node.queueLength += queue.length
            node.expressQueueLength = queue.length
          }
          break
      }
    }
  }

  // for (const node of Object.values(stats.master.nodes)) {
  //   console.log(`=>`, JSON.stringify(node.stats.transactionsInPastMinute, '', 2))
  // }
  // console.log(`stats.master=`, JSON.stringify(stats, '', 2))
  res.send(stats)
  return next();
}
