/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import dbPipelines, { clonePipeline, commitPipelineDraftVersion, deletePipelineVersion, getPipelines, saveDraftPipelineSteps } from '../database/dbPipelines'
import errors from 'restify-errors'
import { db_updatePipelineType, getPipelineType } from '../database/dbTransactionType'
import crypto from 'crypto'

export async function route_getPipelineV1(req, res, next) {
  console.log(`route_getPipelineV1()`)

  const pipelineName = req.params.pipeline
  

  const type = await getPipelineType(pipelineName)
  // console.log(`type=`, type)
  const pipelines = await getPipelines(pipelineName, null)
  // console.log(`JSON.stringify(pipelines, '', 2)=`, JSON.stringify(pipelines, '', 2))
  res.send({ type, pipelines })
  return next()
}

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

/**
 * Clone an existing pipeline version, to create a version named 'draft'.
 * 
 * @param {*} req 
 * @param {*} res 
 * @param {*} next 
 */
export async function clonePipelineV1(req, res, next) {
  console.log(`clonePipelineV1()`)
  console.log(`req.params=`, req.params)
  // // console.log(`req.body.steps=`, req.body.steps)
  // const definition = req.body
  // if (definition.stepType !== STEP_TYPE_PIPELINE) {
  //   const msg = `stepType must be '${STEP_TYPE_PIPELINE}'`
  //   // console.log(`savePipelineDraftV1: ${msg}`)
  //   return next(new errors.BadRequestError(msg))
  // }
  // await dbPipelines.savePipelineDraft(definition)
  const newPipeline = await clonePipeline(req.params.pipeline, req.params.version)
  console.log(`newPipeline=`, newPipeline)
  res.send(newPipeline)
  return next();
}

/**
 * Update the steps for a draft pipeline.
 * 
 * @param {*} req 
 * @param {*} res 
 * @param {*} next 
 * @returns 
 */
export async function savePipelineDraftV1(req, res, next) {
  console.log(`savePipelineDraftV1()`)
  console.log(`req.params=`, req.params)
  console.log(`req.body=`, req.body)

  await saveDraftPipelineSteps(req.params.pipeline, req.body)
  res.send({ status: 'ok' })
  return next();
}

/**
 * Commit the current draft version, to create a version that
 * uses a hash as the version number.
 * 
 * @param {*} req 
 * @param {*} res 
 * @param {*} next 
 */
export async function commitPipelineV1(req, res, next) {
  console.log(`commitPipelineV1()`)
  console.log(`req.params=`, req.params)

  const pipelineName = req.params.pipeline
  const comment = req.body.comment
  const version = await commitPipelineDraftVersion(pipelineName, comment)
  res.send({ version })
  return next();
}

export async function route_deletePipelineVersionV1(req, res, next) {
  // console.log(`route_deletePipelineVersionV1()`)
  const pipelineName = req.params.pipeline
  const version = req.params.version

  // Don't allow the current used pipeline to be deleted.
  const type = await getPipelineType(pipelineName)
  if (type.version === version) {
    res.send(new errors.NotAcceptableError(`Cannot delete pipeline version currently in use`))
    return res.next()
  }

  // Delete the pipeline
  await deletePipelineVersion(pipelineName, version)
  res.send({ status: 'ok' })
  return next()
}


export function generatePipelineHash(pipelineName, stepJson, commitLogJson) {
  // console.log(`---------------------------------`)
  // console.log(`generatePipelineHash()`)
  // console.log(`pipelineName=`, pipelineName)
  // console.log(`stepJson=`, stepJson)
  // console.log(`commitLogJson=`, commitLogJson)

  // Restringify the steps and the commitComments
  let steps
  try {
    steps = JSON.parse(stepJson)
  } catch (e) {
    console.log(`Invalid jsonStep:`, e)
    throw new Error('Invalid jsonStep')
  }

  let commitLog
  try {
    commitLog = JSON.parse(commitLogJson)
  } catch (e) {
    console.log(`Invalid commitLogJson:`, e)
    throw new Error('Invalid commitLogJson')
  }

  // If this is a draft version, it doesn't need a hash
  if (commitLog.length > 0 && commitLog[0].v === 'draft') {
    return 'draft'
  }

  // We ensure that the version in the first commitLog is 'draft'. We'll put
  // it back after we've generated the hash.
  const previousVersion = null
  if (commitLog.length < 1) {
    commitLog.push({ })
  }
  commitLog[0].v = 'VERSION_PLACEHOLDER'
  // console.log(``)
  // console.log(`commitLog[0]=`, commitLog[0])

  // Switch it all back to JSON
  const val1 = pipelineName
  const val2 = JSON.stringify(steps)
  const val3 = JSON.stringify(commitLog)
  // console.log(`val3=`, val3)
  const fullString = val1 + val2 + val3
  // console.log(`val1=`, val1)
  // console.log(`val2=`, val2)
  // console.log(`val3=`, val3)
  const hash = crypto.createHash('sha1').update(fullString).digest('hex');
  // console.log(`hash=`, hash)

  // Path the new hash into the commitLog
  // commitLog[0].v = hash
  // const newCommitLogJson = JSON.stringify(commitLog)

  return hash
}

export async function route_updatePipelineTypeV1(req, res, next) {
  // console.log(`route_updatePipelineTypeV1()`)
  const pipelineName = req.params.pipelineName
  const updates = req.body
  // console.log(`pipelineName=`, pipelineName)
  // console.log(`updates=`, updates)

  // Update the pipelineType details
  await db_updatePipelineType(pipelineName, updates)
  res.send({ status: 'ok' })
  return next()
}

export async function route_exportPipelineVersionV1(req, res, next) {
  // console.log(`route_exportPipelineVersionV1()`)
  const pipelineName = req.params.pipelineName
  const version = req.params.version
  // console.log(`pipelineName=`, pipelineName)
  // console.log(`version=`, version)

  const list = await getPipelines(pipelineName, version)
  const pipeline = list[0]
  // console.log(`pipeline=`, pipeline)

  const hash = generatePipelineHash(pipelineName, pipeline.stepsJson, pipeline.commitComments)
  // console.log(`hash=`, hash)

  // Confirm the version values in the database match this hash.
  const log = JSON.parse(pipeline.commitComments)
  // console.log(`log[0]=`, log[0])
  // console.log(`${pipeline.version}  VS  ${hash}`)
  // console.log(`${log[0].v}  VS  ${hash}`)


  if (pipeline.version != hash || log[0].v !== hash) {
    console.log(`Version does not match hash. Invalid record in the database.`)
    res.send({ status: 'error', message: 'Version hash does not match the pipeline definition.'})
    return next()
  }
  delete pipeline.tags
  delete pipeline.status

  // Prepare the file for returning adn download
  const json = JSON.stringify(pipeline, '', 0)
  const filename = `${pipelineName}-${version}.json`
  const contents = json
  // const filename = `${pipelineName}-${version}.datp`
  // const contents = Buffer.from(json).toString('base64')


  // Buffer.from("SGVsbG8gV29ybGQ=", 'base64').toString('ascii')

  res.send({ status: 'ok', filename, contents })
  return next()
}
