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
  console.log(`pipelineDefinitionV1()`)

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
  console.log(`savePipelineDraftV1()`)
  // const list = await dbPipelines.myPipelines()
  console.log(`req.body=`, req.body)
  const definition = req.body
  if (definition.stepType !== 'pipeline') {
    const msg = `stepType must be 'pipeline'`
    console.log(`savePipelineDraftV1: ${msg}`)
    return next(new errors.BadRequestError(msg))
  }
  await dbPipelines.savePipelineDraft(definition)
  res.send({ status: 'ok' })
  return next();
}
