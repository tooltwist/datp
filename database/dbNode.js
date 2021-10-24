/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'

const VERBOSE = false

export default {
  save
}

async function save(node) {
  if (VERBOSE) {
    console.log(`dbNode.save()`, node)
  }

  // Let's try updating first
  let sql = `UPDATE m_node SET name=?, description=?, type=?, status=?`
  let params = [
    node.name,
    node.description,
    node.type,
    node.status
  ]
  if (node.slaveEndpoint) {
    sql += `, slave_endpoint=?`
    params.push(node.slaveEndpoint)
  }
  if (node.registrationTime) {
    sql += `, registration_time=?`
    params.push(node.registrationTime)
  }
  if (node.lastReportedIn) {
    sql += `, last_reported_in=?`
    params.push(node.lastReportedIn)
  }
  sql += ` WHERE id=?`
  params.push(node.nodeId)

  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  let reply = await query(sql, params)
  if (reply.affectedRows > 0) {
    if (VERBOSE) {
      console.log(`- updated`)
    }
    return
  }

  // Need to insert a new record
  console.log(``)
  console.log(`Registering new node - ${node.name}`)
  let sql1 = `INSERT INTO m_node (id, name, description, type, status`
  let sql2 = `) VALUES (?,?,?,?,? `
  params = [
    node.nodeId,
    node.name,
    node.description,
    node.type,
    node.status
  ]
  if (node.slaveEndpoint) {
    sql1 += `, slave_endpoint`
    sql2 += `,?`
    params.push(node.slaveEndpoint)
  }
  if (node.registrationTime) {
    sql1 += `, registration_time`
    sql2 += `,?`
    params.push(node.registrationTime)
  }
  if (node.lastReportedIn) {
    sql1 += `, last_reported_in`
    sql2 += `,?`
    params.push(node.lastReportedIn)
  }
  sql2 += `)`
  sql = `${sql1}${sql2}`
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  reply = await query(sql, params)
  if (VERBOSE) {
    console.log(`- New node: ${node.nodeId}, ${node.name}`)
  }
}