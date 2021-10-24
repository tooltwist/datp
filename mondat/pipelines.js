/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import dbPipelines from '../database/dbPipelines'
import errors from 'restify-errors'

export async function pipelineDescriptionV1(req, res, next) {
  console.log(`pipelineDescriptionV1()`)

  // A pipeline is a type of step.
  const pipelineName = req.params.pipeline
  const description = await ATP.stepDescription(pipelineName)
  res.send(description)
  return next();
}

export async function pipelineDefinitionV1(req, res, next) {
  // console.log(`pipelineDefinitionV1()`)

  // A pipeline is a type of step.
  const pipelineName = req.params.pipeline
  const definition = await ATP.stepDefinition(pipelineName)
  res.send(definition)
  return next();
}

export async function listPipelinesV1(req, res, next) {
  // console.log(`listPipelinesV1()`)
  const list = await dbPipelines.myPipelines()
  res.send(list)
  return next();
}

export async function savePipelineDraftV1(req, res, next) {
  // console.log(`savePipelineDraftV1()`)
  // console.log(`req.body.steps=`, req.body.steps)
  const definition = req.body
  if (definition.stepType !== 'pipeline') {
    const msg = `stepType must be 'pipeline'`
    // console.log(`savePipelineDraftV1: ${msg}`)
    return next(new errors.BadRequestError(msg))
  }
  await dbPipelines.savePipelineDraft(definition)
  res.send({ status: 'ok' })
  return next();
}
