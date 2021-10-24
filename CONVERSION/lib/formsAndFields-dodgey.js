/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from '../../database/query'
import constants from './constants'
// import providers from '../providers/providers'
import assert from 'assert'


export default {
  deleteParameters,
  saveParameter,
  loadParameters,
  getMapping,
  setMapping,
  getServiceDetails,
  getForms,
  getFields,
}

async function deleteParameters(provider, service, messageType) {
  // Delete the existing definition
  //ZZZZZ Need tenant
  //WHATTHE
  assert(false)
  const sql = `DELETE FROM formservice_field WHERE provider=? AND service=? AND message_type=? AND version=?`
  const params = [provider, service, messageType, constants.SCRATCH_VERSION]
  await query(sql, params)

}

async function saveParameter(provider, service, messageType, sequence, name, type, mandatory) {
  const version = constants.SCRATCH_VERSION
  console.log(`${provider}/${service}/${messageType} => ${sequence},${name} => ${type}${mandatory ? ' MANDATORY' : ''}`)
  //WHATTHE
  assert(false)

  //ZZZZZ Need tenant
  const sql = `INSERT INTO formservice_field (provider, service, message_type, version, sequence, name, type, mandatory) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  const params = [provider, service, messageType, version, sequence, name, type, mandatory]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
}

async function loadParameters(provider, service, messageType, version) {
  console.log(`loadParameters(${provider}, ${service}, ${messageType}, ${version})`)
  //WHATTHE
  assert(false)

  // Delete the existing definition
  //ZZZZZ Need tenant
  const sql = `
    SELECT * FROM formservice_field
    WHERE provider=? AND service=? AND message_type=? AND version=?
    ORDER by sequence`
  const params = [provider, service, messageType, version]
  const result = await query(sql, params)

  // Create an index by name
  const list = []
  const index = {} // name -> record
  for (const p of list) {
    const record = {
      sequence: p.sequence,
      name: p.name,
      type: p.type,
      mandatory: p.mandatory
    }
    list.push(record)
    index[record.name] = record
  }

  return {
    provider,
    service,
    messageType,
    version,
    list,
    index
  }
}

async function getServiceDetails(provider, service) {
  //WHATTHE
  assert(false)
  // Load the mapping records.
  const sql = `
    SELECT provider, service, request_message_version, response_message_version, connector
    FROM provider_service
    WHERE provider=? AND service=?`
  const params = [ provider, service ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)

  if (rows.length < 1) {
    return null
  }

  return {
    provider,
    service,
    requestMessageVersion: rows[0].request_message_version,
    responseMessageVersion: rows[0].response_message_version,
    connector: rows[0].connector
  }
}

async function getForms(tenant, viewName) {
  // console.log(`getForms(${tenant}, ${viewName})`)

  const sql = `
    SELECT tenant, view, description
    FROM formservice_view
    WHERE tenant=? AND view LIKE ?`
    const params = [ tenant, viewName ]
    // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result
}

/**
 * ZZZZ Should have version
 * @param {*} tenant
 * @param {*} viewName
 */
async function getFields(tenant, viewName) {
  // console.log(`formsAndFields.js:getFields(${tenant}, ${viewName})`)

  const sql = `
    SELECT name, type, is_mandatory
    FROM formservice_field
    WHERE tenant=? AND view=?`
  const params = [ tenant, viewName ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const rows = await query(sql, params)
  // console.log(`result=`, result)

  // Convert the result
  const result = [ ]
  for (const row of rows) {
    result.push({
      name: row.name,
      type: row.type,
      mandatory: row.is_mandatory ? true : false,
    })
  }
  return result
}

//ZZFS
async function getMapping(tenant, mappingId, version) {
  // console.log(`formsAndFields.js:getMapping(${tenant}, ${mappingId})`)
  //ZZZ version is not used
  // Load the mapping records.
  const sql = `
    SELECT to_field, converter, source
    FROM formservice_field_mapping
    WHERE tenant=? AND mapping_id=?`
    // const params = [ tenant, provider, service, messageType ]
    const params = [ tenant, mappingId ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const rows = await query(sql, params)
  // console.log(`rows=`, rows)

  // Create the mapping table
  const mapping = []
  for (const row of rows) {
    mapping.push({
      field: row.to_field,
      converter: row.converter,
      source: row.source
    })
  }
  return mapping
}

async function setMapping(tenant, mappingId, version, toField, source, converter) {
  console.log(`formsAndFields.js:addMapping(${tenant}, ${mappingId}, ${version}, ${toField}, ${source}, ${converter})`)

  // Delete any existing mapping to this field
  const sql = `DELETE FROM formservice_field_mapping WHERE tenant=? AND mapping_id=? AND version=? AND to_field=?`
  const params = [ tenant, mappingId, version, toField ]
  console.log(`sql=`, sql)
  console.log(`params=`, params)
  const result = await query(sql, params)
  console.log(`result=`, result)

  // Save the new mapping
  if (source && source !== '-') {
    let sql2 = `INSERT INTO formservice_field_mapping (tenant, mapping_id, version, to_field, source, converter) VALUES (?,?,?,?,?,?)`
    const params2 = [ tenant, mappingId, version, toField, source, converter ]
    console.log(`sql2=`, sql2)
    console.log(`params2=`, params2)
    const result2 = await query(sql2, params2)
    console.log(`result2=`, result2)
  }
}

// /**
//  * ZZZZ Should have version
//  * @param {*} tenant
//  * @param {*} viewName
//  */
// async function getMappingZ(tenant, viewName) {
//   console.log(`getMapping(${tenant}, ${viewName})`)

//   const sql = `
//     SELECT tenant, view, field, converter, source
//     FROM formservice_field_mapping
//     WHERE tenant=? AND view LIKE ?`
//   const params = [ tenant, viewName ]
//   // console.log(`sql=`, sql)
//   // console.log(`params=`, params)
//   const result = await query(sql, params)
//   console.log(`result=`, result)
//   return result
// }
