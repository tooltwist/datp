/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { generatePipelineHash } from '../mondat/pipelines'
import query from './query'

const VERBOSE = false

export default {
  myPipelines,
  getPipelines,
}

export async function myPipelines() {
  // console.log(`myPipelines()`)

  let sql = `SELECT name, version, description FROM atp_pipeline`
  const list = await query(sql)
  if (VERBOSE) {
    console.log(`myPipelines():`, list)
  }
  sql += ` ORDER BY name. version`
  return list
}

export async function getPipelines(name, version=null) {
  // console.log(`dbPipelines.getPipelines(name=${name}, version=${version})`)

  let sql = `SELECT name, version, description, steps_json AS stepsJson, notes, status, commit_comments AS commitComments, tags, notes FROM atp_pipeline WHERE name=?`
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
  console.log(`saveDraftPipelineSteps(${pipelineName})`, steps)

  // const name = definition.name
  // // console.log(`name=`, name)
  // const description = definition.description
  // const notes = definition.notes
  // // console.log(`description=`, description)

  // // console.log(`definition.steps=`, definition.steps)
  // const stepsJson = JSON.stringify(definition.steps, '', 2)
  // // console.log(`stepsJson=`, stepsJson)

  // const nodeName = definition.nodeGroup
  const version = 'draft'
  // const status = 'active'

  steps.forEach(step => delete step.id)
  const json = JSON.stringify(steps)

// const name2 = 'kljqhdlkjshf'

  let sql = `UPDATE atp_pipeline SET steps_json=? WHERE name=? AND version=?`
  let  params = [ json, pipelineName, version ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  let result = await query(sql, params)
  // console.log(`result=`, result)

  if (result.affectedRows === 0) {
    console.log(`Not found`)
    // // Save the pipeline with the next version number
    // sql = `INSERT INTO atp_pipeline (name, version, node_name, description, notes, status, steps_json) VALUES (?, ?, ?, ?, ?, ?)`
    // params = [ name, version, nodeName, description, notes, status, stepsJson ]
    // // console.log(`sql=`, sql)
    // // console.log(`params=`, params)
    // result = await query(sql, params)
    // // console.log(`result=`, result)
  }
}

export async function clonePipeline(name, version) {
  console.log(`clonePipeline(${name}, ${version})`)
  // Get the specified pipeline version
  const pipelines = await getPipelines(name, version)
  // const sql = `SELECT * FROM atp_pipeline WHERE name=? AND version=?`
  // const params = [ name, version ]
  // const rows = await query(sql, params)
  if (pipelines.length < 1) {
    throw new Error(`Unknown pipeline ${name}:${version}`)
  }

  const pipeline = pipelines[0]
  console.log(`pipeline=`, pipeline)

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
  const sql = `INSERT INTO atp_pipeline (name, version, description, steps_json, notes, status, commit_comments, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  const params = [ pipeline.name, pipeline.version, pipeline.description, pipeline.stepsJson, pipeline.notes, pipeline.status, pipeline.commitComments, pipeline.tags ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return pipeline
}

export async function commitPipelineDraftVersion(pipelineName, comment) {
  console.log(`commitPipelineDraftVersion()`)
  // Get the steps and commitlog
  let sql = `SELECT steps_json AS stepsJson, commit_comments AS commitLog FROM atp_pipeline WHERE name=? AND version=?`
  let params = [ pipelineName, 'draft']
  const details = await query(sql, params)
  console.log(`details=`, details)
  if (details.length < 1) {
    throw new Error('Pipeline ${pipelineName}:draft not found')
  }

  // Munge the commitLog
  // - remove the draft commit message
  // - add the new comment
  console.log(`details[0].commitLog=`, details[0].commitLog)
  const log = JSON.parse(details[0].commitLog)
  if (log.length > 0 && log[0].v === 'draft') {
    log.shift()
  }
  log.unshift({ ts: new Date(), v: 'VERSION_PLACEHOLDER', c: comment})
  const newCommitLog = JSON.stringify(log)
  console.log(`newCommitLog=`, newCommitLog)

  // Generate the hash, and insert it into the commitlog
  const hash = generatePipelineHash(pipelineName, details[0].stepsJson, newCommitLog)
  log[0].v = hash
  console.log(`hash=`, hash)
  const newCommitLogJson = JSON.stringify(log)
  console.log(`newCommitLogJson=`, newCommitLogJson)

  // Rename the pipeline and update the commit log.
  sql = `UPDATE atp_pipeline SET version=?, commit_comments=? WHERE name=? AND version=?`
  params = [ hash, newCommitLogJson, pipelineName, 'draft']
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  if (result.affectedRows !== 1) {
    throw new Error(`Could not update atp_pipeline`)
  }
  return hash
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
