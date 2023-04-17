/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import juice from '@tooltwist/juice-client'
import Scheduler2 from '../Scheduler2';
import { schedulerForThisNode } from '../../..';
import { KEYSPACE_NODE_REGISTRATION, RedisLua } from './redis-lua';
import pause from '../../../lib/pause';
const Redis = require('ioredis');

// This adds colors to the String class
require('colors')

const STRING_PREFIX = 'string:::'
const KEYPREFIX_EVENT_QUEUE = 'datp:event-queue:LIST:'
// const KEYSPACE_NODE_REGISTRATION = 'datp:node-registration:RECORD:'
const POP_TIMEOUT = 0

const KEYPREFIX_TRANSACTION_STATE = 'datp:transaction-state:RECORD:'
const KEYPREFIX_STATES_TO_PERSIST = 'datp:transaction-states-to-persist:ZSET:'
const KEYPREFIX_REPEAT_DETECTION = 'datp:repeat-detection:RECORD:'
const KEYPREFIX_TEMPORARY_VALUE = 'datp:temporary-value:RECORD:'

const VERBOSE = 0

// Each node must register itself every minute
export const NODE_REGISTRATION_INTERVAL = 30 // seconds


// REDIS does not let us know when a key was set, but we can ask for the
// time-to-live for a key. We can set the expiry time to this value, which
// is so far forward that the key is never going to expire, but we can use
// it to determine how long the key has existed (i.e. A_VERY_LONG_TIME - TTL).
export const A_VERY_LONG_TIME = 60 * 60 * 24 * 365 * 20 // 20 years in seconds

export const TIME_TILL_GLOBAL_CACHE_ENTRY_IS_PERSISTED_TO_DB = 1000 * 60 * 1 // 20 minutes

let allocatedQueues = 0
let enqueueCounter = 0

// Only get the connection once
const CONNECTION_NONE = 0
const CONNECTION_WAIT = 1
const CONNECTION_READY = 2
let connectionStatus = CONNECTION_NONE


// Connections to REDIS
let connection_dequeue = null // mostly blocking, waiting on queue
let connection_enqueue = null
let connection_admin = null

let redisLua = null



export class RedisQueue {

  // Connections to REDIS
  // #dequeueRedis // mostly blocking, waiting on queue
  // #queueRedis
  // #adminRedis


  // constructor () {
  //   super()
  //   this.#dequeueRedis = null
  //   this.#queueRedis = null
  //   this.#adminRedis = null

  //   console.log(`*********************************************************`)
  //   console.log(`           Allocating REDIS connection ${++allocatedQueues}`)
  //   console.log(`*********************************************************`)
  // }

  static async _checkLoaded() {
    // console.log(`_checkLoaded`)

    // Is this connection already active?
    if (connectionStatus === CONNECTION_READY) {
      if (VERBOSE>1) console.log(`{already connected to REDIS}`.gray)
      return
    } else if  (connectionStatus === CONNECTION_WAIT) {
      // Someone else is already preparing the connection.
      if (VERBOSE>1) console.log(`{waiting for REDIS connection}`.gray)
      while (connectionStatus !== CONNECTION_READY) {
        await pause(50)
      }
      return
    }

    // Okay, we'll take on the job of preparing the connection.
    connectionStatus = CONNECTION_WAIT
    VERBOSE && console.log(`{getting REDIS connection}`.gray)

    const host = await juice.string('redis.host', juice.MANDATORY)
    const port = await juice.integer('redis.port', juice.MANDATORY)
    const password = await juice.string('redis.password', juice.OPTIONAL)


    if (VERBOSE) {
      console.log('----------------------------------------------------')
      console.log('Connecting to REDIS')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('----------------------------------------------------')
    }
    const options = {
      port,
      host,
      enableOfflineQueue: false,
      lazyConnect: true,
    }
    if (password) {
      options.password = password
    }
    const newRedis = new Redis(options)
    if (newRedis === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #1')
    }
    const newRedis2 = new Redis(options)
    if (newRedis2 === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #2')
    }
    const newRedis3 = new Redis(options)
    if (newRedis3 === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #3')
    }


    // console.log(`Set REDIS on error`)
    newRedis.on("error", function(err) {
      console.log('----------------------------------------------------')
      console.log('An error occurred using REDIS connection #1:')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('ERROR=' + err);
      console.log('----------------------------------------------------')
    })
    newRedis2.on("error", function(err) {
      console.log('----------------------------------------------------')
      console.log('An error occurred using REDIS connection #2:')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('ERROR=' + err);
      console.log('----------------------------------------------------')
    })
    newRedis3.on("error", function(err) {
      console.log('----------------------------------------------------')
      console.log('An error occurred using REDIS connection #3:')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('ERROR=' + err);
      console.log('----------------------------------------------------')
    })

    await newRedis.connect()
    await newRedis2.connect()
    await newRedis3.connect()

    connection_dequeue = newRedis
    connection_enqueue = newRedis2
    connection_admin = newRedis3

    // Prepare the LUA scripts
    redisLua = new RedisLua()
    // await redisLua.flushPipelineDefinitions()
    await redisLua.uploadPipelineDefinitions()

    // Ready for action!
    if (VERBOSE>1) console.log(`{REDIS connection ready}`.gray)
    connectionStatus = CONNECTION_READY
  }

  static async getRedisLua() {
    // console.log(`RedisQueue-ioredis.getRedisLua()`)
    await RedisQueue._checkLoaded()
    // console.log(`  redisLua=`, redisLua)
    return redisLua
  }

  /**
   * Add an event at the end of the queue
   * @param {Object} event An object that extends QueueItem
   * @returns The event added to the queue
   */
  static async enqueue(queueName, event) {
    if (VERBOSE) console.log(`RedisCache.enqueue(${queueName}, ${event.eventType}) - ${enqueueCounter++}`.brightBlue)
    // assert(event.fromNodeId) // Must say which node it came from
    // console.log(`event=`, event)
    // console.log(new Error('trace in enqueue').stack)
    if (!queueName) {
      throw new Error('RedisQueue.enqueue: queueName must be specified')
    }
    await RedisQueue._checkLoaded()

    // Prepare what we need to save
    event.fromNodeId = schedulerForThisNode.getNodeId()
    let value
    if (typeof(event) === 'string') {
      value = `${STRING_PREFIX}${event}`
    } else {
      value = JSON.stringify(event)
    }

    // Add it to the queue
    const key = listName(queueName)
    VERBOSE && console.log(`{enqueue ${event.eventType} to ${key}}`.gray)
    await connection_enqueue.rpush(key, value)
  }

  // /**
  //  *
  //  */
  // static async dequeue(queues, numEvents, blocking=true) {
  //   if (VERBOSE) console.log(`RedisCache.dequeue(${queues}, num=${numEvents}, blocking=${blocking})`.brightBlue)
  //   assert(numEvents > 0)

  //   // Check we are connected to REDIS
  //   await RedisQueue._checkLoaded()

  //   // We read from multiple lists
  //   const keys = [ ]
  //   for (const queueName of queues) {
  //     const key = listName(queueName)
  //     keys.push(key)
  //   }

  //   let arr = [ ]
  //   if (blocking) {
  //     // Blocking read. We'll use this in idle mode to sleep until something happens.
  //     const list = keys[0] //ZZZZ temporary hack
  //     arr = await connection_dequeue.blpop(list, POP_TIMEOUT)
  //   } else {

  //     // Non-blocking read of numEvents elements from any of the specified queues
  //     let remaining = numEvents
  //     for (const key of keys) {

  //       if (remaining <= 0) {
  //         break
  //       }

  //       const result = await connection_dequeue.lpop(key, remaining)
  //       if (result) {
  //         if (VERBOSE && result.length > 0) {
  //           console.log(`{dequeued ${result.length} from ${key}}`.gray)
  //         }
  //         arr.push(...result)
  //         remaining -= result.length
  //       }
  //     }
  //     // if (arr.length > 0) console.log(`arr=`, arr)

  //     // LMPOP would be ideal, but isn't available until REDIS v7, which isn't supported by npm module.
  //     // // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
  //     // // See https://redis.io/commands/lmpop
  //     // const params = [ keys.length, ...keys, 'LEFT', numEvents ]
  //     // const reply = await connection_dequeue.lmpop(...params)
  //     // console.log(`reply=`, reply)
  //     // // Return value: Null, or a two-element array with the first element being the name of the
  //     // // key from which elements were popped, and the second element is an array of elements.
  //     // arr = reply ? reply[1] : []
  //   }

  //   if (!arr || arr.length === 0) {
  //     return [ ]
  //   }
  //   // console.log(`arr=`, arr)
  //   // console.log(`Dequeued ${arr.length} of ${numEvents}`)

  //   // Convert from either string or JSON
  //   const convertedArr = [ ]
  //   for (const value of arr) {
  //     // console.log(`  value=`, value)
  //     // console.log(`  value=`, typeof value)
  //     if (value.startsWith(STRING_PREFIX)) {
  //       // This is a non-JSON string
  //       const nval = value.substring(STRING_PREFIX.length)
  //       convertedArr.push(nval)
  //       if (VERBOSE) console.log(`{dequeued ${nval}}`.gray)
  //   } else {
  //       // JSON value
  //       const nval = JSON.parse(value)
  //       convertedArr.push(nval)
  //       if (VERBOSE) console.log(`{dequeued ${nval.eventType}}`.gray)
  //     }
  //   }
  //   // console.log(`dequeued ${convertedArr.length} item from queue`)
  //   return convertedArr
  // }

  /**
   *
   * @returns
   */
  static async queueLengths() {
    // console.log(`getQueueLengths()`)
    await RedisQueue._checkLoaded()
    const keys =  await connection_admin.keys(`${KEYPREFIX_EVENT_QUEUE}*`)
    // console.log(`keys=`, keys)

    const queues = [ ] // [ { nodeGroup, nodeId?, queueLength }]
    if (keys) {
      for (const key of keys) {
        // datp:queue:group:GROUP or datp:queue:node:GROUP:NODEID or datp:queue:express:GROUP:NODEID
        // console.log(`key=>`, key)

        // Get the length
        const length =  await connection_admin.llen(key)

        // Strip the prefix off the list name, to give the application's idea of the queue name.
        // group:GROUP or node:GROUP:NODEID or express:GROUP:NODEID
        const suffix = key.substring(KEYPREFIX_EVENT_QUEUE.length)
        queues.push({ name: suffix, length })
      }
    }
    return queues
  }

  static async queueLength(queueName) {
    // console.log(`queueLength(${queueName})`)

    if (!queueName) {
      throw new Error('RedisQueue.queueLength: queueName must be specified')
    }
    try {
      await RedisQueue._checkLoaded()
      const list = listName(queueName)
      const len =  await connection_admin.llen(list)
      return len
    } catch (e) {
      console.log(`Error in RedisQueue.queueLength: `, e)
    }
  }

  /**
   * Remove all the events in a queue.
   * Use this function VERY CAREFULLY!!!
   * It will cause the removed steps to not be started.
   *
   * @param {string} queueName
   */
  static async drainQueue(queueName) {
    if (VERBOSE) console.log(`RedisQueue.drainQueue(${queueName})`.brightBlue)
    if (!queueName) {
      throw new Error('RedisQueue.queueLength: queueName must be specified')
    }
    // See https://redis.io/commands/ltrim
    await RedisQueue._checkLoaded()
    const list = listName(queueName)
    const len =  await connection_enqueue.llen(list)
    if (VERBOSE) console.log(` - ${await connection_enqueue.llen(list)} before`)
    if (len > 0) {
      // See https://stackoverflow.com/questions/9828160/delete-all-entries-in-a-redis-list
      // await connection_enqueue.ltrim(list, 0, 0)
      await connection_enqueue.del(list)
      // console.log()
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      console.log(`   ${len} events in queue [${list}] have been drained, so will not be run.`.gray)
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      // console.log(``)
    }
    if (VERBOSE) console.log(` - ${await connection_enqueue.llen(list)} after`)
  }

//   /**
//    * Move elements from one queue to another.
//    * This is typically called when a node has died, to move elements from the
//    * element's regular and express queues to the group queue, so they can be
//    * processed by another node.
//    *
//    * @param {string} fromQueue
//    * @param {string} toQueue
//    * @returns Number of elements moved
//    */
//   static async moveElementsToAnotherQueue(fromQueueName, toQueueName) {
//     // console.log(`moveElementsToAnotherQueue(${fromQueueName}, ${toQueueName})`)
//     const fromQueue = listName(fromQueueName)
//     const toQueue = listName(toQueueName)
//     // console.log(`fromQueue=`, fromQueue)
//     // console.log(`toQueue=`, toQueue)
//     for (let i = 0; ; i++) {
//       const value = await connection_admin.lmove(fromQueue, toQueue, 'left', 'right')
//       if (!value) {
//         // None left
//         console.log(`- moved ${i} events.`)
//         return i
//       }
// // return 1
//     }
//   }

  /**
   * Detect if something happens more than once, within the specified number of seconds.
   *  See https://redis.io/commands/incr
   *  See https://redis.io/commands/expire
   */
  static async repeatEventDetection(key, interval) {
    // console.log(`repeatEventDetection(${key}, ${interval})`)
    await RedisQueue._checkLoaded()
    key = `${KEYPREFIX_REPEAT_DETECTION}${key}`
    const count = await connection_admin.incr(key)
    // console.log(`count=`, count)
    // console.log(`count=`, typeof(count))
    await connection_admin.expire(key, interval)
    return (count > 1)
  }

  /**
   * Store a value for _duration_ seconds. During this period the
   * value can be accessed using _getTemporaryValue_. This is commonly used
   * with the following design pattern to cache slow-access information.
   * ```javascript
   * const value = await getTemporaryValue(key)
   * if (!value) {
   *    value = await get_value_from_slow_location()
   *    await setTemporaryValue(key, value, EXOPIRY_TIME_IN_SECONDS)
   * }
   * ```
   * @param {string} key
   * @param {string}} value
   * @param {num} duration Expiry time in seconds
   */
  static async setTemporaryValue(key, value, duration) {
    await RedisQueue._checkLoaded()
    key = `${KEYPREFIX_TEMPORARY_VALUE}${key}`
    if (typeof(value) === 'string') {
      value = `${STRING_PREFIX}${value}`
    } else {
      value = JSON.stringify(value)
    }
    // console.log(`SET value=`, value)
    await connection_admin.set(key, value, 'ex', duration)
  }

  /**
   * Access a value saved using _setTemporaryValue_. If the expiry duration for
   * the temporary value has passed, null will be returned.
   *
   * @param {string} key
   * @returns The value saved using _setTemporaryValue_.
   */
  static async getTemporaryValue(key) {
    await RedisQueue._checkLoaded()
    key = `${KEYPREFIX_TEMPORARY_VALUE}${key}`
    let value = await connection_admin.get(key)
    if (value === null) {
      return null
    }
    else if (value.startsWith(STRING_PREFIX)) {
      return value.substring(STRING_PREFIX.length)
    } else {
      try {
        return JSON.parse(value)
      } catch (e) {
        // Invalid JSON - not much we can do except consider it expired
        console.log(`Corrupt, non-JSON, temporary value in REDIS [${key}]`)
        return null
      }
    }
  }

  /**
   *
   * @param {*} nodeGroup
   * @param {*} nodeId
   * @param {*} status
   */
  static async registerNodeInREDIS(nodeGroup, nodeId, status) {
    // console.log(`registerNodeInREDIS(nodeGroup=${nodeGroup}, nodeId=${nodeId}) status=`, status)
    await RedisQueue._checkLoaded()
    const key = `${KEYSPACE_NODE_REGISTRATION}${nodeGroup}:${nodeId}`
    status.timestamp = Date.now()
    const json = JSON.stringify(status, '', 2)
    await connection_admin.set(key, json, 'ex', NODE_REGISTRATION_INTERVAL + 30)
  }

  /**
   * Return currently active nodes, grouped by nodeGroup.
   *```javascript
   *   [
   *     {
   *       nodeGroup: 'master',
   *       nodes: [ 'nodeId-46988093e9bdaa6c29df4674bf3b8136e7853516' ],
   *       stepTypes: [
   *         {
   *           name: 'example/demo',
   *           description: 'An example of a simple step',
   *           defaultDefinition: [Object],
   *           availableInEveryNode: true
   *         },
   *         ...
   *       ]
   *     },
   *     ...
   *   ]
   * ```
   * If _withStepTypes_ is false or not specified the _stepTypes_ list will
   * be omitted.
   * If a stepType is supported by one or more nodes, but not all nodes, then
   * _availableInEveryNode_ will be false.
   *
   * @param {boolean} withStepTypes Should the reply include step types
   * @returns
   */
  static async getDetailsOfActiveNodesfromREDIS(withStepTypes=false) {
    // console.log(`getDetailsOfActiveNodesfromREDIS(withStepTypes=${withStepTypes})`)
    const keys = await connection_admin.keys(`${KEYSPACE_NODE_REGISTRATION}*`)
    // console.log(`keys=`, keys)

    // Group by nodeGroup
    const groups = { }
    for (const key of keys) {
      // Get the nodeGroup and nodeId from the key
      // e.g. datp:node-registration:RECORD:master:nodeId-e4a22be588913654d4d0b404a3828b55f1b98153
      const arr = key.split(':')
      if (arr.length === 5) {
        const nodeGroup = arr[3]
        // console.log(`nodeGroup=`, nodeGroup)
        const nodeId = arr[4]
        let group = groups[nodeGroup]
        if (!group) {
          group = { nodeGroup, nodes: [ nodeId ], stepTypes: [ ], _nodeDefinitions: { }, warnings: [ ] }
          groups[nodeGroup] = group
        } else {
          group.nodes.push(nodeId)
        }

        // Are we collecting stepTypes also?
        if (withStepTypes) {
          const nodeJSON = await connection_admin.get(key)
          try {
            const definition = JSON.parse(nodeJSON)
            // console.log(`definition=`, definition)
            group._nodeDefinitions[nodeId] = definition
          } catch (e) {
            console.log(`nodeJSON=`, nodeJSON)
            console.log(`Internal error: REDIS contains invalid JSON definition in node registration ${key}`)
            console.log(`e=`, e)
            group._nodeDefinitions[nodeId] = { stepTypes: [ ] }
          }
        }
      } else {
        console.log(`Internal error: REDIS contains invalid JSON definition in node registration ${key}`)
        console.log(`(Does not have 5 sections)`)
      }
    }

    // Combine the stepType definitions for all the nodes in each group, to create the group's stepTypes.
    for (const nodeGroup in groups) {
      const group = groups[nodeGroup]
      RedisQueue._mergeStepTypesInGroup(group)
    }

    // Convert the groups to a list
    const list = [ ]
    for (const nodeGroup in groups) {
      const group = groups[nodeGroup]
      group.nodes.sort() // Sort the nodeIds
      list.push(group)
    }
    list.sort((g1, g2) => {
      const masterFirst1 = (g1.nodeGroup === 'master') ? 0 : 1
      const masterFirst2 = (g2.nodeGroup === 'master') ? 0 : 1
      if (masterFirst1 < masterFirst2) return -1
      if (masterFirst1 > masterFirst2) return +1
      if (g1.nodeGroup < g2.nodeGroup) return -1
      if (g1.nodeGroup > g2.nodeGroup) return +1
      return 0
    })

    // console.log(`RedisQueue-ioredis.getDetailsOfActiveNodesfromREDIS():`, JSON.stringify(list, '', 2))
    return list
  }

  static _mergeStepTypesInGroup(group) {
    const types = { } // stepTypeName => stepTypeDefinition

    // We add a '_nodeCount' to the step definition and increment it for each node with the stepType
    let nodeCount = 0
    for (let nodeId in group._nodeDefinitions) {
      nodeCount++
      const definition = group._nodeDefinitions[nodeId]
      // console.log(`definition=`, definition)
      for (const st of definition.stepTypes) {
        const name = st.name
        const type = types[name]
        if (type) {
          type.count++
        } else {
          st.count = 1
          types[name] = st
          // console.log(`st=`, st)
        }
      }//- for
    }//- for

    // Add the types to the group
    for (let name in types) {
      const type = types[name]
      group.stepTypes.push(type)
      type.availableInEveryNode = (type.count === nodeCount)
      delete type.count
    }
    delete group._nodeDefinitions
    // console.log(`group=`, group)
  }// _mergeStepTypesInGroup

  /**
   *
   * @returns { stepTypes }
   */
  static async getNodeDetailsFromREDIS(nodeGroup, nodeId) {
    // console.log(`getNodeDetailsFromREDIS(${nodeGroup}, ${nodeId})`)
    const key = `${KEYSPACE_NODE_REGISTRATION}${nodeGroup}:${nodeId}`
    const json = await connection_admin.get(key)
    // console.log(`json=`, json)
    try {
      const status = JSON.parse(json)
      return status

    } catch (e) {
      console.log(`Internal error: REDIS contains invalid JSON definition in node registration ${key}`)
      console.log(`e=`, e)
      return null
    }
  }

  /**
   *
   */
  static async close() {
    if (connection_enqueue) {
      VERBOSE && console.log(`{disconnecting from REDIS}`.gray)
      // See https://github.com/luin/ioredis/blob/master/API.md#clusterquitcallback--promise
      await connection_enqueue.quit()
      await connection_admin.quit()
      connection_enqueue = null
      connection_admin = null
    }
  }


  // /**
  //  * Store a transaction to REDIS.
  //  * 
  //  * @param {TransactionState} transaction
  //  * @param {persistAfter} duration Time before it is written to long term storage (seconds)
  //  */
  // static async saveTransactionState_level1(transactionState) {
  //   // console.log(`saveTransaction(${transactionState.getTxId()}, persistAfter=${persistAfter}) - ${transactionState.getDeltaCounter()}`)
  //   // console.log(`typeof(transactionState)=`, typeof(transactionState))
  //   // console.log(`transactionState=`, transactionState)

  //   await RedisQueue._checkLoaded()

  //   // Save the transactionState as JSON
  //   const txId = transactionState.getTxId()
  //   const json = transactionState.stringify()
  //   // await connection_admin.set(key, value, 'ex', duration)

  //   // console.log(`SAVING TO REDIS: json=`, JSON.stringify(JSON.parse(json), '', 2))
  //   // console.log(`SAVINF ${json.length} TO REDIS WITH KEY ${key}`)
  //   const key = `${KEYPREFIX_TRANSACTION_STATE}${txId}`
  //   // await connection_admin.set(key, json, 'ex', A_VERY_LONG_TIME)
  //   await connection_admin.pipeline().set(key, json, 'ex', A_VERY_LONG_TIME).exec()


  //   // Add it to a queue to be persisted to the database later (by a separate server)
  //   // See https://developpaper.com/redis-delay-queue-ive-completely-straightened-it-out-this-time/
  //   const persistTimeMs = Date.now() + TIME_TILL_GLOBAL_CACHE_ENTRY_IS_PERSISTED_TO_DB
  //   await connection_admin.zadd(KEYPREFIX_STATES_TO_PERSIST, persistTimeMs, txId)
  // }

  // /**
  //  * Working in conjunction with _saveTransactionState_, this function is run
  //  * periodically to find transaction states that need to be shifted from the
  //  * global (REDIS) cache, up to long term storage in the database.
  //  */
  // static async persistTransactionStatesToLongTermStorage() {
  //   // console.log(`RedisQueue-ioredis::persistTransactionStatesToLongTermStorage()`)

  //   await RedisQueue._checkLoaded()

  //   // // Save the transactionState as JSON
  //   // const txId = transactionState.getTxId()
  //   // const json = transactionState.stringify()
  //   // // await connection_admin.set(key, value, 'ex', duration)

  //   // // console.log(`SAVING TO REDIS: json=`, JSON.stringify(JSON.parse(json), '', 2))
  //   // // console.log(`SAVINF ${json.length} TO REDIS WITH KEY ${key}`)
  //   // const key = `${KEYPREFIX_TRANSACTION_STATE}${txId}`
  //   // await connection_admin.set(key, json, 'ex', A_VERY_LONG_TIME)

  //   // Add it to a queue to be persisted to the database later (by a separate server)
  //   // See https://developpaper.com/redis-delay-queue-ive-completely-straightened-it-out-this-time/
  //   const min = '-inf'
  //   const max = Date.now()
  //   const result = await connection_admin.zrange(KEYPREFIX_STATES_TO_PERSIST, min, max, 'BYSCORE')
  //   // console.log(`result=`, result)

  //   for (const txId of result) {
  //     if (VERBOSE) console.log(`Persisting state of ${txId} to long term storage`)
  //     const key = `${KEYPREFIX_TRANSACTION_STATE}${txId}`
  //     let json = await connection_admin.get(key)
  //     if (json === null) {
  //       // Transaction state not in the REDIS cache
  //       // May have been deleted by another server also doing the persisting.
  //       await connection_admin.zrem(KEYPREFIX_STATES_TO_PERSIST, txId)
  //       continue
  //     }

  //     // console.log(`json=`, json)

  //     await archiveTransactionState(txId, json)

  //     // try {
  //     //   /*
  //     //    *  Insert transaction state into the database.
  //     //    */
  //     //   let sql = `INSERT INTO atp_transaction_state (transaction_id, json) VALUES (?, ?)`
  //     //   let params = [ txId, json ]
  //     //   let result2 = await dbupdate(sql, params)
  //     //   // console.log(`result2=`, result2)
  //     //   if (result2.affectedRows !== 1) {
  //     //     //ZZZZZZ Notify the admin
  //     //     console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not insert into DB [${txId}]`, e)
  //     //     continue
  //     //   }

  //     // } catch (e) {
  //     //   if (e.code !== 'ER_DUP_ENTRY') {
  //     //     //ZZZZZZ Notify the admin
  //     //     console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not insert into DB [${txId}]`, e)
  //     //     continue
  //     //   }

  //     //   /*
  //     //    *  Already in DB - need to update
  //     //    */
  //     //   // console.log(`Need to update`)
  //     //   const sql = `UPDATE atp_transaction_state SET json=? WHERE transaction_id=?`
  //     //   const params = [ json, txId ]
  //     //   const result2 = await dbupdate(sql, params)
  //     //   // console.log(`result2=`, result2)
  //     //   if (result2.affectedRows !== 1) {
  //     //     //ZZZZZZ Notify the admin
  //     //     console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not update DB [${txId}]`, e)
  //     //     continue
  //     //   }
  //     // }

  //     // We've either inserted or updated the database.
  //     // We can now remove from REDIS.
  //     if (VERBOSE) console.log(`Removing transaction state from REDIS [${txId}]`)
  //     await connection_admin.zrem(KEYPREFIX_STATES_TO_PERSIST, txId)
  //     await connection_admin.del(key)

  //     // If we are shutting down now, quit immediately.
  //     if (schedulerForThisNode.shuttingDown()) {
  //       return
  //     }
  //   }// next txId
  // }

  // /**
  //  * Access a value saved using _setTemporaryValue_. If the expiry duration for
  //  * the temporary value has passed, null will be returned.
  //  *
  //  * @param {string} key
  //  * @returns The value saved using _setTemporaryValue_.
  //  */
  // static async getTransactionState(txId) {
  //   await RedisQueue._checkLoaded()
  //   const key = `${KEYPREFIX_TRANSACTION_STATE}${txId}`
  //   let json = await connection_admin.get(key)
  //   if (json === null) {
  //     // Transaction state not in the REDIS cache
  //     return null
  //   } else {
  //     return new TransactionState(json)
  //   }
  // }//- getTransactionState

  static async queueStats() {

    /*
     *  Get details of transaction states.
     */
    let states = await connection_admin.keys(`${KEYPREFIX_TRANSACTION_STATE}*`)
    const transactionStates = states.length
    const transactionStatesPending = await connection_admin.zcount(KEYPREFIX_STATES_TO_PERSIST, '-inf', '+inf')

    /*
     *    Get details of node groups and nodes.
     */
    const groups = [ ]
    const findGroup = function (nodeGroup) {
      let group = groups.find(group => (group.nodeGroup === nodeGroup))
      if (!group) {
        group = { nodeGroup, nodes: [ ] }
        groups.push(group)
      }
      return group
    }
    const findNode = function (group, nodeId) {
      let node = group.nodes.find(node => (node.nodeId === nodeId))
      if (!node) {
        node = { nodeId }
        group.nodes.push(node)
      }
      return node
    }

    // Get details from the node keep-alives in REDIS
    let nodes = await connection_admin.keys(`${KEYSPACE_NODE_REGISTRATION}*`)
    // console.log(`nodes=`, nodes)
    for (const key of nodes) {
      const arr = key.split(':')
      const nodeGroup = arr[3]
      const nodeId = arr[4]
      // console.log(`nodeGroup=`, nodeGroup)
      const group = findGroup(nodeGroup)
      // console.log(`nodeId=`, nodeId)
      const node = findNode(group, nodeId)

      const value = await connection_admin.get(key)
      try {
        const keepalive = JSON.parse(value)
        // console.log(`keepalive=`, keepalive)
        // console.log(`zzbzbzbzbzbzbbz`)
        node.events = keepalive.events
        node.workers = keepalive.workers
        node.throughput = keepalive.throughput
      } catch (e) {
        // Must have just expired
        node.events = 0
        node.workers = 0
        node.throughput = 0
      }
    }
    // console.log(`groups=`, groups)
    groups.sort((g1, g2) => {
      const m1 = g1.nodeGroup === 'master' ? 0 : 1
      const m2 = g2.nodeGroup === 'master' ? 0 : 1
      if (m1 < m2) return -1
      if (m1 > m2) return +1
      if (g1.nodeGroup < g2.nodeGroup) return -1
      if (g1.nodeGroup > g2.nodeGroup) return +1
      return 0
    })

    // Collect the group information
    for (const group of groups) {

      // Check the group queue size
      const queueKey = listName(Scheduler2.groupQueueName(group.nodeGroup))
      group.queueLen = await connection_admin.llen(queueKey)

      // Get the queue sizes for the individual nodes
      for (const node of group.nodes) {
        // Check the group queue size
        const queueKey = listName(Scheduler2.nodeRegularQueueName(group.nodeGroup, node.nodeId))
        node.queueLen = await connection_admin.llen(queueKey)
        const expessQueueKey = listName(Scheduler2.nodeExpressQueueName(group.nodeGroup, node.nodeId))
        node.expressQueueLen = await connection_admin.llen(expessQueueKey)
      }

      // Sort the nodes
      group.nodes.sort((n1, n2) => {
        if (n1.nodeId < n2.nodeId) return -1
        if (n1.nodeId > n2.nodeId) return +1
        return 0
      })
    }

    // let queues = await connection_admin.keys(`${KEYPREFIX_EVENT_QUEUE}*`)
    let queues = await connection_admin.keys(`*`)
    // console.log(`queues=`, queues)

    const stats = {
      transactionStates,
      transactionStatesPending,
      groups
    }

    return stats
  }

}// RedisQueue

// Work out the list name
function listName(queueName) {
  return `${KEYPREFIX_EVENT_QUEUE}${queueName}`
}
