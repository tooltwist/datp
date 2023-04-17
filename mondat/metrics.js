/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
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
