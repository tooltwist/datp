/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

/**
 * We originally had a mapping between transaction type and pipelines.
 * This is now changed so the transaction type and pipeline are the same.
 * This script does the conversion, renaming pipelines to match their corresponding transaction type.
 */

import dbupdate from '../database/dbupdate'
import query from '../database/query'


async function main() {

  // Look for pipelines mapped to more then one transaction type
  const pipelineNames = await query(`SELECT DISTINCT(name) FROM atp_pipeline`)
  // console.log(`pipelineNames=`, pipelineNames)

  let errs = 0
  const unmappedPipelines = [ ]
  for (const pipeline of pipelineNames) {
    const rows = await query(`SELECT * FROM atp_transaction_type WHERE pipeline_name = ?`, [ pipeline.name ])
    if (rows.length === 0) {
      unmappedPipelines.push(pipeline.name)
    } else if (rows.length > 1) {
      console.log(`Error: Pipeline ${pipeline.name} is used by ${rows.length} transaction types.`)
      errs++
      for (const type of rows) {
        console.log(`  - ${type.transaction_type}`)
      }
    }
  }
  if (errs) {
    console.log(`Will not continue.`)
    process.exit(1)
  }

  // How about pipelines mapped to no transaction type
  console.log(`Pipelines with no transaction type record:`)
  let cnt = 0
  for (const pipeline of unmappedPipelines) {
      console.log(`- ${pipeline}`)
  }
  for (const pipeline of unmappedPipelines) {
    const sql = `INSERT INTO atp_transaction_type (transaction_type, pipeline_name, pipeline_version) VALUES (?,?,?)`
    const params = [ pipeline, pipeline, '1.0']
    await dbupdate(sql, params)
    cnt++
  }
  if (cnt === 0) {
    console.log(`  (none)`)
  } else {
    console.log(`New transaction types created`)
  }

  // Each pipeline is now mapped to, and only one, transaction type.
  // If the transaction type and pipeline name do not match, rename the pipeline.
  // We'll remove the pipeline_name fields soon.
  const diffs = await query(`SELECT transaction_type, pipeline_name FROM atp_transaction_type WHERE transaction_type != pipeline_name`)
  // console.log(`diffs=`, diffs)
  for (const rec of diffs) {
    // const transactionType = rec.
    const sql = `UPDATE atp_pipeline SET name=? WHERE name=?`
    const params = [ rec.transaction_type, rec.pipeline_name]
    console.log(`sql=`, sql)
    console.log(`params=`, params)
    await dbupdate(sql, params)

    await dbupdate(`UPDATE atp_transaction_type SET pipeline_name=? WHERE transaction_type=?`, [ rec.transaction_type, rec.transaction_type])
  }

  // At this point, the transaction types and the pipeline names correspond.
  // If a transaction type has no description, use the description of the pipeline.
  // We'll remove pipeline descriptions soon.
  console.log(`Patching in descriptions into atp_transaction_type records, where required:`)
  const noDesc = await query(`SELECT transaction_type FROM atp_transaction_type WHERE description = '' OR description IS NULL`)
  // console.log(`noDesc=`, noDesc)
  for (const noDescTxType of noDesc) {
    // console.log(`noDescTxType=`, noDescTxType)
    // Find a pipeline with a description
    const pipelines = await query(`SELECT name, description FROM atp_pipeline WHERE name=?`, [ noDescTxType.transaction_type ])
    // console.log(`pipelines=`, pipelines)
    let desc = null
    for (const pl of pipelines) {
      if (pl.description) {
        desc = pl.description
        break
      }
    }
    if (desc) {
      console.log(`  - ${noDescTxType.transaction_type}: ${desc}`)
      await dbupdate(`UPDATE atp_transaction_type SET description=? WHERE transaction_type=?`, [ desc, noDescTxType.transaction_type ])
    }
  }
  process.exit(0)
}



main().then().catch(e => { console.log(`Bummer: `, e) })