/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors';
import dbStep from '../database/dbStep';

export async function mondatRoute_getRecentPerformanceV1(req, res, next) {
  // console.log(`mondatRoute_getRecentPerformanceV1()`)
  // console.log(`req.params=`, req.params)
  // console.log(`req.query=`, req.query)

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
