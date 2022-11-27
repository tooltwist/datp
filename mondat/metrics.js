/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors';
import { RedisQueue } from '../ATP/Scheduler2/queuing/RedisQueue-ioredis';
import dbStep from '../database/dbStep';

export async function mondatRoute_getMetricsV1(req, res, next) {
  console.log(`mondatRoute_getMetricsV1()`)
  console.log(`req.params=`, req.params)
  console.log(`req.query=`, req.query)

  const redisLua = await RedisQueue.getRedisLua()
  const metrics = await redisLua.getMetrics()
  res.send(metrics)
  return next()

  const nodeId = req.params.nodeId
  // console.log(`nodeId=`, nodeId)

  let seconds = 60
  if (req.query.duration) {
    try {
      seconds = parseInt(req.query.duration)
    } catch (e) {
      console.log(`e=`, e)
      return next(new errors.InvalidArgumentError('Invalid duration'))
    }
  }
  const rows = await dbStep.getRecentPerformance(seconds)

  // A pipeline is a type of step.
  // const pipelineName = req.params.pipeline
  // const description = await ATP.stepDescription(pipelineName)
  res.send(rows)
  return next();
}
