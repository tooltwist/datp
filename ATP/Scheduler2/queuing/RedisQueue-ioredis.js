/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { QueueManager } from './QueueManager';
import juice from '@tooltwist/juice-client'
import assert from 'assert'
import { stat } from 'fs';
const Redis = require('ioredis');
// const util = require('util');

// This adds colors to the String class
require('colors')

const STRING_PREFIX = 'string:::'
const REDIS_LIST_PREFIX = 'datp:queue:'
const NODE_REGISTRATION_PREFIX = 'datp:node:'
const POP_TIMEOUT = 0

const VERBOSE = 0

// Each node must register itself every minute
export const NODE_REGISTRATION_INTERVAL = 30 // seconds

let allocatedQueues = 0

export class RedisQueue extends QueueManager {

  // Connections to REDIS
  #dequeueRedis // mostly blocking, waiting on queue
  #queueRedis
  #adminRedis


  constructor () {
    super()
    this.#dequeueRedis = null
    this.#queueRedis = null
    this.#adminRedis = null

    console.log(`*********************************************************`)
    console.log(`           Allocating REDIS connection ${++allocatedQueues}`)
    console.log(`*********************************************************`)
  }

  async _checkLoaded() {
    // console.log(`_checkLoaded`)

    // Is this connection already active?
    if (this.#queueRedis) {
      (VERBOSE>1) && console.log(`{already connected to REDIS}`.gray)
      return
    }
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
    const options = { port, host }
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

    this.#dequeueRedis = newRedis
    this.#queueRedis = newRedis2
    this.#adminRedis = newRedis3
  }

  /**
   * Add an event at the end of the queue
   * @param {Object} event An object that extends QueueItem
   * @returns The event added to the queue
   */
  async enqueue(queueName, event) {
    if (VERBOSE) console.log(`RedisCache.enqueue(${queueName}, ${event.eventType})`.brightBlue)
    assert(event.fromNodeId) // Must say which node it came from
    // console.log(`event=`, event)
    // console.log(new Error('trace in enqueue').stack)
    if (!queueName) {
      throw new Error('RedisQueue.enqueue: queueName must be specified')
    }
    await this._checkLoaded()

    // Prepare what we need to save
    let value
    if (typeof(event) === 'string') {
      value = `${STRING_PREFIX}${event}`
    } else {
      value = JSON.stringify(event)
    }

    // Add it to the queue
    const key = listName(queueName)
    VERBOSE && console.log(`{enqueue ${event.eventType} to ${key}}`.gray)
    await this.#queueRedis.rpush(key, value)
  }

  /**
   *
   */
  async dequeue(queues, numEvents, blocking=true) {
    if (VERBOSE) console.log(`RedisCache.dequeue(${queues}, num=${numEvents}, blocking=${blocking})`.brightBlue)

    // Check we are connected to REDIS
    await this._checkLoaded()

    // We read from multiple lists
    const keys = [ ]
    for (const queueName of queues) {
      const key = listName(queueName)
      keys.push(key)
    }

    let arr = [ ]
    if (blocking) {
      // Blocking read. We'll use this in idle mode to sleep until something happens.
      const list = keys[0] //ZZZZ temporary hack
      arr = await this.#dequeueRedis.blpop(list, POP_TIMEOUT)
    } else {

      // Non-blocking read of numEvents elements from any of the specified queues
      let remaining = numEvents
      for (const key of keys) {

        const result = await this.#dequeueRedis.lpop(key, remaining)
        if (result) {
          if (VERBOSE && result.length > 0) {
            console.log(`{dequeued ${result.length} from ${key}}`.gray)
          }
          arr.push(...result)
          remaining -= result.length
        }
      }
      // if (arr.length > 0) console.log(`arr=`, arr)

      // LMPOP would be ideal, but isn't available until REDIS v7, which isn't supported by npm module.
      // // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
      // // See https://redis.io/commands/lmpop
      // const params = [ keys.length, ...keys, 'LEFT', numEvents ]
      // const reply = await this.#dequeueRedis.lmpop(...params)
      // console.log(`reply=`, reply)
      // // Return value: Null, or a two-element array with the first element being the name of the
      // // key from which elements were popped, and the second element is an array of elements.
      // arr = reply ? reply[1] : []
    }

    if (!arr || arr.length === 0) {
      return [ ]
    }
    // console.log(`arr=`, arr)
    // console.log(`Dequeued ${arr.length} of ${numEvents}`)

    // Convert from either string or JSON
    const convertedArr = [ ]
    for (const value of arr) {
      // console.log(`  value=`, value)
      // console.log(`  value=`, typeof value)
      if (value.startsWith(STRING_PREFIX)) {
        // This is a non-JSON string
        const nval = value.substring(STRING_PREFIX.length)
        convertedArr.push(nval)
        if (VERBOSE) console.log(`{dequeued ${nval}}`.gray)
    } else {
        // JSON value
        const nval = JSON.parse(value)
        convertedArr.push(nval)
        if (VERBOSE) console.log(`{dequeued ${nval.eventType}}`.gray)
      }
    }
    // console.log(`dequeued ${convertedArr.length} item from queue`)
    return convertedArr
  }

  /**
   *
   * @returns
   */
  async queueLengths() {
    // console.log(`getQueueLengths()`)
    await this._checkLoaded()
    const keys =  await this.#adminRedis.keys(`${REDIS_LIST_PREFIX}*`)
    // console.log(`keys=`, keys)

    const queues = [ ] // [ { nodeGroup, nodeId?, queueLength }]
    if (keys) {
      for (const key of keys) {
        // datp:queue:group:GROUP or datp:queue:node:GROUP:NODEID or datp:queue:express:GROUP:NODEID
        // console.log(`key=>`, key)

        // Get the length
        const length =  await this.#adminRedis.llen(key)

        // Strip the prefix off the list name, to give the application's idea of the queue name.
        // group:GROUP or node:GROUP:NODEID or express:GROUP:NODEID
        const suffix = key.substring(REDIS_LIST_PREFIX.length)
        queues.push({ name: suffix, length })
      }
    }
    return queues
  }

  async queueLength(queueName) {
    // console.log(`queueLength(${queueName})`)

    if (!queueName) {
      throw new Error('RedisQueue.queueLength: queueName must be specified')
    }
    try {
      await this._checkLoaded()
      const list = listName(queueName)
      const len =  await this.#adminRedis.llen(list)
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
  async drainQueue(queueName) {
    if (VERBOSE) console.log(`RedisQueue.drainQueue(${queueName})`.brightBlue)
    if (!queueName) {
      throw new Error('RedisQueue.queueLength: queueName must be specified')
    }
    // See https://redis.io/commands/ltrim
    await this._checkLoaded()
    const list = listName(queueName)
    const len =  await this.#queueRedis.llen(list)
    if (VERBOSE) console.log(` - ${await this.#queueRedis.llen(list)} before`)
    if (len > 0) {
      // See https://stackoverflow.com/questions/9828160/delete-all-entries-in-a-redis-list
      // await this.#queueRedis.ltrim(list, 0, 0)
      await this.#queueRedis.del(list)
      // console.log()
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      console.log(`   ${len} events in queue [${list}] have been drained, so will not be run.`.gray)
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      // console.log(``)
    }
    if (VERBOSE) console.log(` - ${await this.#queueRedis.llen(list)} after`)
  }

  /**
   * Move elements from one queue to another.
   * This is typically called when a node has died, to move elements from the
   * element's regular and express queues to the group queue, so they can be
   * processed by another node.
   *
   * @param {string} fromQueue
   * @param {string} toQueue
   * @returns Number of elements moved
   */
  async moveElementsToAnotherQueue(fromQueueName, toQueueName) {
    console.log(`moveElementsToAnotherQueue(${fromQueueName}, ${toQueueName})`)
    const fromQueue = listName(fromQueueName)
    const toQueue = listName(toQueueName)
    for (let i = 0; ; i++) {
      const value = await this.#adminRedis.lmove(fromQueue, toQueue, 'left', 'right')
      if (!value) {
        // None left
        return i
      }
// return 1
    }
  }

  /**
   * Detect if something happens more than once, within the specified number of seconds.
   *  See https://redis.io/commands/incr
   *  See https://redis.io/commands/expire
   */
  async repeatEventDetection(key, interval) {
    // console.log(`repeatEventDetection(${key}, ${interval})`)
    await this._checkLoaded()
    key = `datp:repeat-detection:${key}`
    const count = await this.#adminRedis.incr(key)
    // console.log(`count=`, count)
    // console.log(`count=`, typeof(count))
    await this.#adminRedis.expire(key, interval)
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
  async setTemporaryValue(key, value, duration) {
    await this._checkLoaded()
    key = `datp:temporary-value:${key}`
    await this.#adminRedis.set(key, value, 'ex', duration)
  }

  /**
   * Access a value saved using _setTemporaryValue_. If the expiry duration for
   * the temporary value has passed, null will be returned.
   *
   * @param {string} key
   * @returns The value saved using _setTemporaryValue_.
   */
  async getTemporaryValue(key) {
    await this._checkLoaded()
    key = `datp:temporary-value:${key}`
    const value = await this.#adminRedis.get(key)
    return value
  }

  /**
   *
   * @param {*} nodeGroup
   * @param {*} nodeId
   * @param {*} status
   */
  async registerNode(nodeGroup, nodeId, status) {
    // console.log(`registerNode(${nodeGroup}, ${nodeId})`)
    await this._checkLoaded()
    const key = `${NODE_REGISTRATION_PREFIX}${nodeGroup}:${nodeId}`
    status.timestamp = Date.now()
    const json = JSON.stringify(status, '', 2)
    await this.#adminRedis.set(key, json, 'ex', NODE_REGISTRATION_INTERVAL + 30)
  }

  /**
   *
   */
  async getNodeIds() {
    // console.log(`getNodeIds()`)
    const keys = await this.#adminRedis.keys(`${NODE_REGISTRATION_PREFIX}*`)
    // console.log(`keys=`, keys)

    // Group by nodeGroup
    const groups = { }
    for (const key of keys) {
      // Get the nodeGroup and nodeId from the key
      const arr = key.split(':')
      if (arr.length === 4) {
        const nodeGroup = arr[2]
        const nodeId = arr[3]
        if (!groups[nodeGroup]) {
          groups[nodeGroup] = { nodeGroup, nodes: [ nodeId ] }
        } else {
          groups[nodeGroup].nodes.push(nodeId)
        }
      } else {
        console.log(`Internal error: REDIS contains invalid JSON definition in node registration ${key}`)
      }
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
    // console.log(`list=`, list)
    return list
  }

  /**
   *
   * @returns { stepTypes }
   */
  async getNodeDetails(nodeGroup, nodeId) {
    const key = `${NODE_REGISTRATION_PREFIX}${nodeGroup}:${nodeId}`
    const json = await this.#adminRedis.get(key)
    console.log(`json=`, json)
    try {
      const status = JSON.parse(nodeJSON)
      // status.nodeGroup = nodeGroup
      // stat.nodeId = nodeId
      console.log(`status=`, status)
      return status
    } catch (e) {
      console.log(`Internal error: REDIS contains invalid JSON definition in node registration ${key}`)
      return null
    }
  }

  /**
   *
   */
  async close() {
    if (this.#queueRedis) {
      VERBOSE && console.log(`{disconnecting from REDIS}`.gray)
      // See https://github.com/luin/ioredis/blob/master/API.md#clusterquitcallback--promise
      await this.#queueRedis.quit()
      await this.#adminRedis.quit()
      this.#queueRedis = null
      this.#adminRedis = null
    }
  }

}// RedisQueue

// Work out the list name
function listName(queueName) {
  return `${REDIS_LIST_PREFIX}${queueName}`
}
