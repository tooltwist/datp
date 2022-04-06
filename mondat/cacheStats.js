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
import { getNodeGroups } from '../database/dbNodeGroup';

export async function routeCacheStatsV1(req, res, next) {
  // console.log(`routeCacheStatsV1()`)

  const stats = await schedulerForThisNode.cacheStats()

  res.send(stats)
  return next();
}
