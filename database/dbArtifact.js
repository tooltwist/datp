import query from './query'

export default {
  saveArtifact,
  artifactsForStep,
}


async function saveArtifact(stepId, name, text) {
  // console.log(`dbArtifact.save(${stepId}, ${name})`)

  if (typeof(text) !== 'string') {
    throw new Error('saveArtifact() requires string value for json parameter')
  }
  const sql = `INSERT INTO atp_step_artifact (step_id, artifact_name, value) VALUES (?, ?, ?)`
  const params = [ stepId, name, text ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
}

async function artifactsForStep(stepId) {
  console.log(`artifactsForStep(${stepId})`)
  const sql = `SELECT artifact_name AS name, json_value AS value FROM atp_step_artifact WHERE step_id=? ORDER BY name`
  const params = [ stepId ]
  console.log(`sql=`, sql)
  console.log(`params=`, params)
  const result = await query(sql, params)
  console.log(`result=`, result)
  return result
}