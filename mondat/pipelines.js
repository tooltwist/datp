/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import dbPipelines, { clonePipeline, commitPipelineDraftVersion, createPipeline, db_getPipelineTypesV1, db_importPipelineVersion, deletePipelineVersion, getPipelines, saveDraftPipelineSteps } from '../database/dbPipelines'
import errors from 'restify-errors'
import { db_updatePipelineType, getPipelineType } from '../database/dbTransactionType'
import crypto from 'crypto'

export async function mondatRoute_getPipelinesTypesV1(req, res, next) {
  console.log(`mondatRoute_getPipelinesTypesV1()`.brightRed)
  const list = await db_getPipelineTypesV1()
  // console.log(`list=`, list)

  res.send(list)
  return next();
}

export async function mondatRoute_getPipelineV1(req, res, next) {
  // console.log(`mondatRoute_getPipelineV1()`)

  const pipelineName = req.params.pipeline
  const type = await getPipelineType(pipelineName)
  // console.log(`type=`, type)
  const pipelines = await getPipelines(pipelineName, null)
  // console.log(`JSON.stringify(pipelines, '', 2)=`, JSON.stringify(pipelines, '', 2))
  res.send({ type, pipelines })
  return next()
}

export async function mondatRoute_pipelineDescriptionV1(req, res, next) {
  // console.log(`mondatRoute_pipelineDescriptionV1()`)

  // A pipeline is a type of step.
  const pipelineName = req.params.pipeline
  const description = await ATP.stepDescription(pipelineName)
  res.send(description)
  return next();
}

export async function mondatRoute_pipelineDefinitionV1(req, res, next) {
  // console.log(`mondatRoute_pipelineDefinitionV1()`)

  // A pipeline is a type of step.
  const pipelineName = req.params.pipeline
  const definition = await ATP.stepDefinition(pipelineName)
  res.send(definition)
  return next();
}

export async function mondatRoute_listPipelinesV1(req, res, next) {
  // console.log(`mondatRoute_listPipelinesV1()`)
  const list = await dbPipelines.myPipelines()
  res.send(list)
  return next();
}

/**
 * Clone an existing pipeline version, to create a version named 'draft'.
 */
export async function mondatRoute_clonePipelineV1(req, res, next) {
  // console.log(`mondatRoute_clonePipelineV1()`)
  // console.log(`req.params=`, req.params)
  const newPipeline = await clonePipeline(req.params.pipeline, req.params.version)
  // console.log(`newPipeline=`, newPipeline)
  res.send(newPipeline)
  return next();
}

/**
 * Update the steps for a draft pipeline.
 */
export async function mondatRoute_savePipelineDraftV1(req, res, next) {
  // console.log(`mondatRoute_savePipelineDraftV1()`)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)

  await saveDraftPipelineSteps(req.params.pipeline, req.body)
  res.send({ status: 'ok' })
  return next();
}

/**
 * Commit the current draft version, to create a version that uses a hash as the version number.
 */
export async function mondatRoute_commitPipelineV1(req, res, next) {
  // console.log(`mondatRoute_commitPipelineV1()`)
  // console.log(`req.params=`, req.params)

  const pipelineName = req.params.pipeline
  const comment = req.body.comment
  const version = await commitPipelineDraftVersion(pipelineName, comment)
  res.send({ version })
  return next();
}

/**
 * Delete a version of a pipeline
 */
export async function mondatRoute_deletePipelineVersionV1(req, res, next) {
  // console.log(`mondatRoute_deletePipelineVersionV1()`)
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
}//- mondatRoute_deletePipelineVersionV1

/**
 * Generate a unique hash based on a pipeline definition.
 */
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
}//- generatePipelineHash

/**
 * Update pipeline definition.
 */
export async function mondatRoute_updatePipelineTypeV1(req, res, next) {
  // console.log(`mondatRoute_updatePipelineTypeV1()`)
  const pipelineName = req.params.pipelineName
  const updates = req.body
  // console.log(`pipelineName=`, pipelineName)
  // console.log(`updates=`, updates)

  // Update the pipelineType details
  await db_updatePipelineType(pipelineName, updates)
  res.send({ status: 'ok' })
  return next()
}//- mondatRoute_updatePipelineTypeV1

export async function mondatRoute_exportPipelineVersionV1(req, res, next) {
  // console.log(`mondatRoute_exportPipelineVersionV1()`)
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



  res.send({ status: 'ok', filename, contents })
  return next()
}//- mondatRoute_exportPipelineVersionV1

/**
 * Export a pipeline definition.
 */
export async function mondatRoute_importPipelineVersionV1(req, res, next) {
  console.log(`mondatRoute_importPipelineVersionV1()`)
  const pipelineName = req.params.pipelineName
  const data = req.body
  // console.log(`pipelineName=`, pipelineName)
  // console.log(`data=`, data)
  const filename = data.filename
  const type = data.type
  const contents = data.contents

  let json = contents
  let shortendName
  if (filename.endsWith('.datp')) {
    json = Buffer.from(contents, 'base64').toString('ascii')
    // console.log(`json=`, json)
    shortendName = filename.substring(0, filename.length - '.datp'.length)
  } else if (filename.endsWith('.json')) {
    json = contents
    shortendName = filename.substring(0, filename.length - '.json'.length)
  } else {
    res.send({ status: 'Invalid filename extension (should be .json or .datp' })
    return next()
  }
  // console.log(`shortendName=`, shortendName)


  // See if it is valid
  let pipeline = null
  let hash = null
  try {
    pipeline = JSON.parse(json)

    // Get the hash for this pipeline
    hash = generatePipelineHash(pipelineName, pipeline.stepsJson, pipeline.commitComments)
  } catch (e) {
    //ZZZZZ This should be written to the admin log
    console.log(`Attempt to import invalid JSON file`)
    console.log(`e=`, e)
    res.send({ status: 'Invalid pipeline definition file' })
    return next()
  }
  // console.log(`hash=`, hash)

  // See what the version is, in the filename
  let warning = ''
  let hyphenPos = shortendName.indexOf('-')
  if (hyphenPos >= 0) {
    let version = shortendName.substring(hyphenPos + 1)
    // console.log(`version=`, version)

    // Macs will add a suffix for duplicate files. e.g. example-draft (2).json
    let spacePos = version.indexOf(' ')
    if (spacePos > 0) {
      version = version.substring(0, spacePos)
    }
    // console.log(`version=`, version)

    // See if the version in the filename matches the hash
    if (version !== hash) {
      console.log(`The version does not match the hash`)
      warning += `The version in the filename does not match the hash for the pipeline.\n`
      warning += `Will be loaded as version ${hash}\n`
    }
  }

  // See if we have this version already
  const existing = await getPipelines(pipelineName, hash)
  // console.log(`existing=`, existing)
  if (existing.length > 0) {
    warning += `Cannot be loaded - version ${hash} already exists.`
  } else {
    // Save to the database
    const newPipeline = {
      name: pipelineName,
      version: hash,
      stepsJson: pipeline.stepsJson,
      status: 'inactive',
      commitComments: pipeline.commitComments,
      notes: pipeline.notes,
    }
    const hash2 = await db_importPipelineVersion(newPipeline)
    if (hash2 === null) {
      warning += `Cannot be loaded - version ${hash} already exists.`
    }
  }

  res.send({ status: 'ok', message: warning })
  return next()
}//- mondatRoute_importPipelineVersionV1

export async function mondatRoute_newPipelineTypeV1(req, res, next) {
  console.log(`mondatRoute_newPipelineTypeV1()`)
  console.log(`req.params=`, req.params)

  const name = req.params.pipelineName.trim()
  await createPipeline(name)
  res.send({ status: 'ok' })
  return next()
}