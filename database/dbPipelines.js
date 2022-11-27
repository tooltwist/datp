/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { generatePipelineHash } from '../mondat/pipelines'
import dbupdate from './dbupdate'
import query from './query'

const VERBOSE = 0

const PIPELINE_CACHE_REFRESH_TIME_MS = 120 * 1000 // Retain entries for two minutes only
const pipelineCache = [ ] // pipelineName => { timestamp, { pipelineStuff }}

export default {
  myPipelines,
  getPipelines,
  getPipelineVersionInUse,
}

export async function db_getPipelineTypesV1 () {
  // console.log(`db_getPipelineTypesV1()`)

  let sql = `SELECT
    transaction_type AS name,
    is_transaction_type AS isTransactionType,
    description,
    pipeline_version AS pipelineVersion,
    node_group AS nodeGroup,
    notes
  FROM atp_transaction_type`
  const list = await query(sql)
  if (VERBOSE) {
    console.log(`myPipelines():`, list)
  }
  sql += ` ORDER BY name. version`
  return list
}


export async function myPipelines() {
  // console.log(`myPipelines()`)

  let sql = `SELECT name, version FROM atp_pipeline`
  const list = await query(sql)
  if (VERBOSE) {
    console.log(`myPipelines():`, list)
  }
  sql += ` ORDER BY name. version`
  return list
}

export async function getPipelineVersionInUse(name) {
  // if (VERBOSE)
  console.trace(`dbPipelines.getPipelineVersionInUse(name=${name})`)

  // This should be cached
  let rec = pipelineCache[name]
  const now = Date.now()
  if (rec && (now - rec.timestamp) < PIPELINE_CACHE_REFRESH_TIME_MS) {
    // console.log(`pipeline details found in cache`)
    return rec.pipelineStuff
  }
  // console.log(`pipeline details NOT found in cache`)

  // Not in the cache, load the pipeline details now. The transaction type
  // dictates the pipeline version and the node group where it will run.
  let sql = `SELECT
    T.node_group AS nodeGroup,
    P.name,
    P.version,
    P.steps_json AS stepsJson,
    P.notes,
    P.status,
    P.commit_comments AS commitComments,
    P.tags,
    P.notes
  FROM atp_pipeline P
  INNER JOIN atp_transaction_type T
    ON T.transaction_type = P.name AND P.version = T.pipeline_version
  WHERE P.name=?`
  let params = [ name ]

  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)
  if (VERBOSE) {
    console.log(`getPipelineVersionInUse():`, rows)
  }
  const pipelineStuff = (rows.length > 0) ? rows[0] : null

  // Add to the cache (even if it is null)
  pipelineCache[name] = { timestamp: now, pipelineStuff }
  return pipelineStuff
}


export async function getPipelines(name, version=null) {
  if (VERBOSE) console.log(`dbPipelines.getPipelines(name=${name}, version=${version})`)

  let sql = `SELECT name, version, steps_json AS stepsJson, notes, status, commit_comments AS commitComments, tags, notes FROM atp_pipeline WHERE name=?`
  let params = [ name ]
  if (version) {
    sql +=  ` AND version=?`
    params.push(version)
  }
  sql += ` ORDER BY version`
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const list = await query(sql, params)
  if (VERBOSE) {
    console.log(`getPipelines():`, list)
  }
  return list
}

export async function saveDraftPipelineSteps(pipelineName, steps) {
  if (VERBOSE) console.log(`saveDraftPipelineSteps(${pipelineName})`, steps)

  const version = 'draft'
  steps.forEach(step => delete step.id)
  const json = JSON.stringify(steps)

// const name2 = 'kljqhdlkjshf'

  let sql = `UPDATE atp_pipeline SET steps_json=? WHERE name=? AND version=?`
  let  params = [ json, pipelineName, version ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  let result = await dbupdate(sql, params)
  // console.log(`result=`, result)

  if (result.affectedRows === 0) {
    console.log(`Not found`)
    // // Save the pipeline with the next version number
    // sql = `INSERT INTO atp_pipeline (name, version, node_name, description, notes, status, steps_json) VALUES (?, ?, ?, ?, ?, ?)`
    // params = [ name, version, nodeName, description, notes, status, stepsJson ]
    // // console.log(`sql=`, sql)
    // // console.log(`params=`, params)
    // result = await dbupdate(sql, params)
    // // console.log(`result=`, result)
  }
}

/**
 * Clone an existing pipeline, as the starting point for a new pipeline / version.
 */
export async function clonePipeline(name, version) {
  if (VERBOSE) console.log(`clonePipeline(${name}, ${version})`)
  // Get the specified pipeline version
  const pipelines = await getPipelines(name, version)
  // const sql = `SELECT * FROM atp_pipeline WHERE name=? AND version=?`
  // const params = [ name, version ]
  // const rows = await query(sql, params)
  if (pipelines.length < 1) {
    throw new Error(`Unknown pipeline ${name}:${version}`)
  }

  const pipeline = pipelines[0]
  // console.log(`pipeline=`, pipeline)

  // Adjust the commit comments
  let cc = [ ]
  if (pipeline.commitComments) {
    try {
      cc = JSON.parse(pipeline.commitComments)
    } catch (e) {
      // Start from scratch
    }
  }
  cc.unshift({ ts: new Date(), v: 'draft', m: 'draft version'})
  pipeline.commitComments = JSON.stringify(cc)

  // Save the draft pipeline
  pipeline.version = 'draft'
  pipeline.status = 'active'
  pipeline.tags = null

  // Create the new 
  const sql = `INSERT INTO atp_pipeline (name, version, steps_json, notes, status, commit_comments, tags) VALUES (?, ?, ?, ?, ?, ?, ?)`
  const params = [ pipeline.name, pipeline.version, pipeline.stepsJson, pipeline.notes, pipeline.status, pipeline.commitComments, pipeline.tags ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  // console.log(`result=`, result)
  return pipeline
}//- clonePipeline

export async function commitPipelineDraftVersion(pipelineName, comment) {
  // console.log(`commitPipelineDraftVersion()`)
  // Get the steps and commitlog
  let sql = `SELECT steps_json AS stepsJson, commit_comments AS commitLog FROM atp_pipeline WHERE name=? AND version=?`
  let params = [ pipelineName, 'draft']
  const details = await query(sql, params)
  // console.log(`details=`, details)
  if (details.length < 1) {
    throw new Error('Pipeline ${pipelineName}:draft not found')
  }

  // Munge the commitLog
  // - remove the draft commit message
  // - add the new comment
  // console.log(`details[0].commitLog=`, details[0].commitLog)
  const log = JSON.parse(details[0].commitLog)
  if (log.length > 0 && log[0].v === 'draft') {
    log.shift()
  }
  log.unshift({ ts: new Date(), v: 'VERSION_PLACEHOLDER', c: comment})
  const newCommitLog = JSON.stringify(log)
  // console.log(`newCommitLog=`, newCommitLog)

  // Generate the hash, and insert it into the commitlog
  const hash = generatePipelineHash(pipelineName, details[0].stepsJson, newCommitLog)
  log[0].v = hash
  // console.log(`hash=`, hash)
  const newCommitLogJson = JSON.stringify(log)
  // console.log(`newCommitLogJson=`, newCommitLogJson)

  // Rename the pipeline and update the commit log.
  sql = `UPDATE atp_pipeline SET version=?, commit_comments=? WHERE name=? AND version=?`
  params = [ hash, newCommitLogJson, pipelineName, 'draft']
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  // console.log(`result=`, result)
  if (result.affectedRows !== 1) {
    throw new Error(`Could not update atp_pipeline`)
  }
  return hash
}

export async function db_importPipelineVersion(pipeline) {
  // console.log(`db_importPipelineVersion()`)

  // Munge the commitLog
  // - overwrite the version number
  // console.log(`pipeline.commitComments=`, pipeline.commitComments)
  const log = JSON.parse(pipeline.commitComments)
  const newCommitComments = JSON.stringify(log)
  // console.log(`newCommitComments=`, newCommitComments)

  // Generate the hash, and insert it into the commitlog
  const hash = generatePipelineHash(pipeline.name, pipeline.stepsJson, newCommitComments)
  log[0].v = hash
  // console.log(`hash=`, hash)
  // const newCommitCommentsJson = JSON.stringify(log)
  // console.log(`newCommitCommentsJson=`, newCommitCommentsJson)

  try {
    // Rename the pipeline and update the commit log.
    const sql = `INSERT INTO atp_pipeline (name, version, steps_json, status, commit_comments, notes) VALUES (?, ?, ?, ?, ?, ?)`
    const params = [ pipeline.name, pipeline.version, pipeline.stepsJson, pipeline.status, pipeline.commitComments, pipeline.notes ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await dbupdate(sql, params)
    // console.log(`result=`, result)
    return hash
  } catch (e) {
    // console.log(`e=`, e)
    // console.log(`JSON.stringify(e, '', 2)=`, JSON.stringify(e, '', 2))
    if (e.code === 'ER_DUP_ENTRY') {
      // We can ignore this.
      return null
    } else {
      throw e
    }
  }
}

export async function deletePipelineVersion(pipelineName, version) {
  const sql = `DELETE FROM atp_pipeline WHERE name=? AND version=?`
  const params = [ pipelineName, version ]
  const result = await query(sql, params)
  // console.log(`result=`, result)
  if (result.affectedRows !== 1) {
    throw new Error(`Could not delete atp_pipeline`)
  }
}

/**
 * Create a new pipeline, by creating it's initial draft version. 
 */
export async function createPipeline(name) {
  if (VERBOSE) console.log(`createPipeline(${name})`)

  // Save the draft pipeline
  const version = 'draft'
  const stepsJson = JSON.stringify([
    {
      "id": 1,
      "definition": {
        "min": 50,
        "max": 500,
        "stepType": "util/delay",
        "description": "Delay a random period of time"
      }
    }
  ])
  const notes = ''
  const status = 'draft'
  const commitComments = JSON.stringify([ { ts: new Date(), v: 'draft', m: 'draft version'} ])
  const tags = null
  const nodeGroup = 'master'

  // Create the new transaction type
  const sql2 = `INSERT INTO atp_transaction_type (transaction_type, is_transaction_type, description, pipeline_version, node_group, notes, pipeline_name) VALUES (?, ?, ?, ?, ?, ?, ?)`
  const params2 = [ name, 1, '', version, nodeGroup, '', name ]
  console.log(`sql2=`, sql2)
  console.log(`params2=`, params2)
  const result2 = await dbupdate(sql2, params2)
  console.log(`result2=`, result2)

  // Create the new pipeline
  const sql = `INSERT INTO atp_pipeline (name, version, steps_json, notes, status, commit_comments, tags) VALUES (?, ?, ?, ?, ?, ?, ?)`
  const params = [ name, version, stepsJson, notes, status, commitComments, tags ]
  console.log(`sql=`, sql)
  console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  console.log(`result=`, result)
}//- createPipeline
