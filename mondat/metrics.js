/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { luaGetMetrics } from '../ATP/Scheduler2/queuing/redis-metrics';

export async function mondatRoute_getMetricsV1(req, res, next) {
  console.log(`mondatRoute_getMetricsV1()`)
  console.log(`req.params=`, req.params)
  console.log(`req.query=`, req.query)

  const metrics = await luaGetMetrics()
  res.send(metrics)
  return next()
}
