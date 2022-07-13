/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import dbupdate from "../database/dbupdate"
import query from "../database/query"


/**
 * Rename the step type. If newName is null, it reports, but does not change the pipelines.
 * @param {String} oldName Existing steptype
 * @param {String | null} newName What the step type will be called in future.
 */
async function stepTypeRenamed(oldName, newName) {
  // console.log(`oldName=`, oldName)
  // console.log(`newName=`, newName)

  const pipelines = await query(`select * from atp_pipeline`)
  // console.log(`pipelines=`, pipelines)

  let changedSteps = 0
  let changedPipelines = 0
  for (const pipeline of pipelines) {
    const json = pipeline.steps_json
    let steps
    try {
      steps = JSON.parse(json)
    } catch (e) {
      console.log(`Corrupt pipeline JSON [${pipeline.name}]: ${e}`)
      console.log(`json=`, json)
      continue
    }
    // console.log(`steps=`, steps)
    let pipelineChanged = false
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i]
      // console.log(`step=`, step)
      const type = step.definition.stepType
      // console.log(`  - ${step.definition.stepType}`)

      if (type === oldName) {
        console.log(`step #${i} - ${pipeline.name}`)
        changedSteps++

        if (newName) {
          // console.log(`broken step =`, JSON.stringify(step.definition, '', 2))
          step.definition.stepType = newName
          pipelineChanged = true
        }
      }
    }//- next step

    if (pipelineChanged) {
      try {
        // console.log(`corrected step =`, JSON.stringify(step.definition, '', 2))
        const stepsJson = JSON.stringify(steps, '', 2)
        // console.log(`stepsJson=`, stepsJson)
        const sql2 = `UPDATE atp_pipeline SET steps_json=? WHERE name=?`
        const params2 = [ stepsJson, pipeline.name ]
        await dbupdate(sql2, params2)
        changedPipelines++
      } catch (e) {
        console.log(`BIG PROBLEM. COULD NOT UPDATE PIPELINE ${pipeline.name}:`, e)
        process.exit(1)
      }
    }
  }
  console.log(`Changed ${changedSteps} steps in ${changedPipelines} of ${pipelines.length} pipelines.`)
}



(async function main() {
  // console.log(`main()`, process.argv)

  const oldName = process.argv[2]
  const newName = (process.argv.length > 3) ? process.argv[3] : null
  await stepTypeRenamed(oldName, newName)
  process.exit(0)
})().then(() => {
  // Do nothing
}).catch(e => {
  console.log(`e=`, e)
  process.exit(0)
})