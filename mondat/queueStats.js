/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from '..';
import { RedisQueue } from '../../DATP/ATP/Scheduler2/queuing/RedisQueue-ioredis';
import { luaQueueStats } from '../ATP/Scheduler2/queuing/redis-metrics';
// import TransactionCacheAndArchive from '../ATP/Scheduler2/TransactionCacheAndArchive';
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

  const result = await luaQueueStats()
  // console.log(`luaQueueStats(): result=`, result)
  res.send(result)
  next();
}
