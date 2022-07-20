/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'
import assert from 'assert'
import dbupdate from './dbupdate'

const VERBOSE = 0

/**
 *
 * @param {Object} group { name, description, hostMondat, serveMondatWebapp, defaultWorkers, delayWhenBusy, delayWhenEmpty, loopDelay, idleModeTime }
 * @returns
 */
export async function saveNodeGroup(group) {
  if (VERBOSE)  console.log(`dbNodeGroup.saveNodeGroup()`, group)
  assert(group.nodeGroup)
  const nodeGroup = group.nodeGroup

  const mapping = {
    description: 'description',
    hostMondat: 'host_mondat',
    serveMondatWebapp: 'serve_mondat_api',
    numWorkers: 'num_workers',
    eventloopPause: 'eventloop_pause',
    eventloopPauseBusy: 'eventloop_pause_busy',
    eventloopPauseIdle: 'eventloop_pause_idle',
    delayToEnterSlothMode: 'sloth_mode_delay',
    debugScheduler: 'debug_scheduler',
    debugWorkers: 'debug_workers',
    debugSteps: 'debug_steps',
    debugPipelines: 'debug_pipelines',
    debugRouters: 'debug_routers',
    debugLongpolling: 'debug_longpolling',
    debugWebhooks: 'debug_webhooks',
    debugTransactions: 'debug_transactions',
    debugTransactionCache: 'debug_transaction_cache',
    debugRedis: 'debug_redis',
    debugDb: 'debug_db',
  }

  let sql = 'UPDATE atp_node_group SET'
  let params = [ ]
  let sep = ' '
  for (let property in group) {
    if (property !== 'nodeGroup') {
      const value = group[property]
      const column = mapping[property]
      if (column) {
        sql += `${sep}${column}=?`
        params.push(value)
        sep = ', '
      } else {
        throw new Error(`saveNodeGroup: unknown property ${property}`)
      }
    }
  }
  sql += ` WHERE node_group=?`
  params.push(group.nodeGroup)

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
  // console.log(``)
  // console.log(`Registering new group - ${group.nodeGroup}`)
  let sql1 = `INSERT INTO atp_node_group (node_group`
  let sql2 = `) VALUES (?`
  params = [ group.nodeGroup ]
  for (let property in group) {
    if (property !== 'nodeGroup') {
      const value = group[property]
      const column = mapping[property]
      if (column) {
        sql1 += `, ${column}`
        sql2 += `,?`
        params.push(value)
      } else {
        throw new Error(`saveNodeGroup: unknown property ${property}`)
      }
    }
  }
  sql2 += `)`
  sql = `${sql1}${sql2}`
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  reply = await dbupdate(sql, params)
  if (VERBOSE) {
    console.log(`- New group: ${group.nodeGroup} - ${group.description}`)
  }
}

export async function getNodeGroups() {
  const sql = `SELECT node_group AS nodeGroup,
    description,
    host_mondat AS hostMondat,
    serve_mondat_api AS serveMondatWebapp,
    num_workers AS numWorkers,
    eventloop_pause AS eventloopPause,
    eventloop_pause_busy AS eventloopPauseBusy,
    eventloop_pause_idle AS eventloopPauseIdle,
    sloth_mode_delay AS delayToEnterSlothMode,
    debug_scheduler AS debugScheduler,
    debug_workers AS debugWorkers,
    debug_steps AS debugSteps,
    debug_pipelines AS debugPipelines,
    debug_routers AS debugRouters,
    debug_longpolling AS debugLongpolling,
    debug_webhooks AS debugWebhooks,
    debug_transactions AS debugTransactions,
    debug_transaction_cache AS debugTransactionCache,
    debug_redis AS debugRedis,
    debug_db AS debugDb
  FROM atp_node_group`
  return await query(sql)
}

export async function getNodeGroup(nodeGroup) {
  const sql = `SELECT node_group AS nodeGroup,
    description,
    host_mondat AS hostMondat,
    serve_mondat_api AS serveMondatWebapp,
    num_workers AS numWorkers,
    eventloop_pause AS eventloopPause,
    eventloop_pause_busy AS eventloopPauseBusy,
    eventloop_pause_idle AS eventloopPauseIdle,
    sloth_mode_delay AS delayToEnterSlothMode,
    debug_scheduler AS debugScheduler,
    debug_workers AS debugWorkers,
    debug_steps AS debugSteps,
    debug_pipelines AS debugPipelines,
    debug_routers AS debugRouters,
    debug_longpolling AS debugLongpolling,
    debug_webhooks AS debugWebhooks,
    debug_transactions AS debugTransactions,
    debug_transaction_cache AS debugTransactionCache,
    debug_redis AS debugRedis,
    debug_db AS debugDb
  FROM atp_node_group WHERE node_group=?`
  const params = [ nodeGroup ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)
  return rows.length > 0 ? rows[0] : null
}

export async function deleteNodeGroup(nodeGroup) {
  const sql = `DELETE FROM atp_node_group WHERE node_group=?`
  const params = [ nodeGroup ]
  const result = await query(sql, params)
  return (result.affectedRows > 0)
}
