/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'
import me from '../ATP/me'

const VERBOSE = false

export default {
  myPipelines,
  getPipelines,
  savePipelineDraft,
}

export async function myPipelines() {
  // console.log(`myPipelines()`)

  const sql = `SELECT name, version, description FROM atp_pipeline`
  const list = await query(sql)
  if (VERBOSE) {
    console.log(`myPipelines():`, list)
  }
  return list
}

export async function getPipelines(name, version) {
  let sql = `SELECT name, node_name AS nodeName, version, description, status, steps_json AS stepsJson FROM atp_pipeline WHERE name=?`
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
    console.log(`getPipeline():`, list)
  }
  return list
}

export async function savePipelineDraft(definition) {
  // console.log(`savePipelineDraft()`, definition)

  const name = definition.name
  // console.log(`name=`, name)
  const description = definition.description
  // console.log(`description=`, description)

  // console.log(`definition.steps=`, definition.steps)
  const stepsJson = JSON.stringify(definition.steps, '', 2)
  // console.log(`stepsJson=`, stepsJson)

  const nodeName = await me.getName()
  // const version = 'draft'
  // const status = 'draft'
  const version = '1.0'//ZZZZZ
  const status = 'active'

// const name2 = 'kljqhdlkjshf'

  let sql = `UPDATE atp_pipeline SET node_name=?, description=?, status=?, steps_json=? WHERE name=? AND version=?`
  let  params = [ nodeName, description, status, stepsJson, name, version ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  let result = await query(sql, params)
  // console.log(`result=`, result)

  if (result.affectedRows === 0) {
    console.log(`Not found`)
    // Save the pipeline with the next version number
    sql = `INSERT INTO atp_pipeline (name, version, node_name, description, status, steps_json) VALUES (?, ?, ?, ?, ?, ?)`
    params = [ name, version, nodeName, description, status, stepsJson ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    result = await query(sql, params)
    // console.log(`result=`, result)
  }
}

//ZZZZZ Remove this
export async function createInitialPipelinesHack() {
  console.log(`createInitialPipelinesHack()`)

  // One-off initialization
  const zPipelines = [
    {
      name: 'example',
      version: '1.0',
      description: 'Example pipeline'
    },
    {
      name: 'remittance-init',
      version: '1.0',
      description: 'Initiate a remittance'
    },
    {
      name: 'remittance-status',
      version: '1.0',
      description: 'Check the status of a remittance'
    },
    {
      name: 'remittance-cebuana',
      version: '1.0',
      description: 'Remittance to Cebuana Lhuillier'
    },
    {
      name: 'remittance-iremit',
      version: '1.0',
      description: 'Remittance with iRemit'
    },
    {
      name: 'remittance-landbank',
      version: '1.0',
      description: 'Remittance with Landbank'
    },
    {
      name: 'remittance-metrobank',
      version: '1.0',
      description: 'Remittance with Metrobank'
    },
    {
      name: 'remittance-pera',
      version: '1.0',
      description: 'Remittance with PeraHUB'
    },
    {
      name: 'remittance-smartpedala',
      version: '1.0',
      description: 'Remittance with Smart Pedala'
    },
    {
      name: 'remittance-transfast',
      version: '1.0',
      description: 'Remittance with Transfast'
    },
    {
      name: 'remittance-ussc',
      version: '1.0',
      description: 'Remittance with USSC'
    },
    {
      name: 'remittance-wu',
      version: '1.0',
      description: 'Remittance to Western Union'
    },
    {
      name: 'remittance-xpressMoney',
      version: '1.0',
      description: 'Remittance with Xpress Money'
    },
    {
      name: 'wallet-deposit-init',
      version: '1.0',
      description: 'Initiate a transfer'
    },
    {
      name: 'wallet-deposit-status',
      version: '1.0',
      description: 'Check the status of a Transfer'
    },
  ]

  const steps =  [
    {
      "id": "#1",
      "definition": {
        "stepType": "mock",
        "msg": "Validate remittance transaction"
      }
    },
    {
      "id": "#2",
      "definition": {
        "stepType": "mock",
        "msg": "KYC Check"
      }
    },
    {
      "id": "#3",
      "definition": {
        "stepType": "mock",
        "msg": "Fraud check - high"
      }
    },
    {
      "id": "#4",
      "definition": {
        "stepType": "mock",
        "msg": "Duplicate transaction check"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Convert - Western Union Format"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Convert - Western Union Format"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Queue (3 concurrent)"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "backend: Western Union"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Convert reply - Western Union Format"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Accounts Dept. data feed"
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "delay",
        "min": 1000,
        "max": 4000
      }
    },
    {
      "id": "#5",
      "definition": {
        "stepType": "mock",
        "msg": "Finished now."
      }
    }
  ]
  const json = JSON.stringify(steps, '', 2)

  try {
    let sql = `INSERT INTO atp_pipeline (node_name, name, version, description, status, steps_json)\n`
    let sep = `VALUES `
    let params = [ ]
    for (const rec of zPipelines) {
      sql += `${sep}(?, ?, ?, ?, ?, ?)`
      params.push('master')
      params.push(rec.name)
      params.push(rec.version)
      params.push(rec.description)
      params.push('active')
      params.push(json)
      sep = ',\n'
    }
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result  = await query(sql, params)
    // console.log(`result=`, result)
  } catch (e) {
    console.log(`Error:`, e)
  }

}
