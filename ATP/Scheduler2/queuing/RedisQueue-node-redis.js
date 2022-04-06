/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { QueueManager } from './QueueManager';
import juice from '@tooltwist/juice-client'

// Using https://github.com/redis/node-redis  (Not ioredis)
import { createClient } from 'redis'

// This adds colors to the String class
require('colors')

const STRING_PREFIX = 'string:::'
const EVENT_QUEUE_PREFIX = 'datp:queue:'
const POP_TIMEOUT = 0

const VERBOSE = 1

let numQueues = 0

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
    console.log(`           Allocating REDIS connection ${++numQueues}`)
    console.log(`*********************************************************`)
  }

  async _checkLoaded() {
    // console.log(`_checkLoaded`)

    // Is this connection already active?
    if (this.#queueRedis) {
      // VERBOSE && console.log(`{already connected to REDIS}`.gray)
      return
    }

    VERBOSE && console.log(`{getting REDIS connection}`.gray)

    console.log(`initializing REDIS`)

    const host = await juice.string('redis.host', juice.MANDATORY)
    const port = await juice.integer('redis.port', juice.MANDATORY)
    const username = await juice.string('redis.password', juice.OPTIONAL)
    const password = await juice.string('redis.password', juice.OPTIONAL)

    // e.g. redis://alice:foobared@awesome.redis.server:6380
    const userbit = (username && password) ? `${username}:${password}@` : ''
    const url = `redis://${userbit}${host}:${port}`
    if (VERBOSE) {
      console.log('----------------------------------------------------')
      console.log('Connecting to REDIS')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('----------------------------------------------------')
    }
console.log(`url=`, url)//ZZZZZZ
    const options = { port, host }
    if (password) {
      options.password = password
    }
    const newRedis = createClient({ url })
    const newRedis2 = createClient({ url })
    const newRedis3 = createClient({ url })
    // if (VERBOSE) {
    //   console.log('----------------------------------------------------')
    // }

    if (newRedis === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #1')
    }
    if (newRedis2 === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #2')
    }
    if (newRedis3 === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS connection #3')
    }


    // Perhaps report all REDIS communications
    // if (VERBOSE > 1) {
    //   newRedis.monitor(function (err, monitor) {
    //     // Entering monitoring mode.
    //     console.log(`Set REDIS on monitor`)
    //     monitor.on("monitor", function (time, args, source, database) {
    //       console.log('   =>  ' + time + ": " + util.inspect(args));
    //     });
    //   });
    // }

    // console.log(`Set REDIS on error`)
    const errorHandler = (err) => {
      console.log('----------------------------------------------------')
      console.log('An error occurred using REDIS:')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('ERROR=', err);
      console.log('----------------------------------------------------')
    }
    newRedis.on("error", errorHandler)
    newRedis2.on("error", errorHandler)
    newRedis3.on("error", errorHandler)

    await newRedis.connect()
    await newRedis2.connect()
    await newRedis3.connect()

    this.#dequeueRedis = newRedis
    this.#queueRedis = newRedis2
    this.#adminRedis = newRedis3
  }

  /**
   * Add an item at the end of the queue
   * @param {QueueItem} item An object that extends QueueItem
   * @returns The item added to the queue
   */
  async enqueue(queueName, item) {
    if (VERBOSE) console.log(`RedisCache.enqueue(${queueName})`.brightBlue)
    // console.log(`item=`, item)
    // console.log(new Error('trace in enqueue').stack)
    if (!queueName) {
      throw new Error('RedisQueue.enqueue: queueName must be specified')
    }
    await this._checkLoaded()

    // Prepare what we need to save
    if (typeof(item) === 'string') {
      item = `${STRING_PREFIX}${item}`
    } else {
      item = JSON.stringify(item)
    }

    // Add it to the list
    const key = listName(queueName)
    VERBOSE && console.log(`{enqueue to ${key}}`.gray)
    // console.log(`WAARP enqueue`, item)
    await this.#queueRedis.RPUSH(key, item)
  }

  /**
   * @returns The item at the head of the queue, removed from the queue
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
    console.log(`keys=`, keys)

    // console.log(`dequeue from ${list}`)
    // let value = null
    let arr = [ ]
    if (blocking) {
      // Read
      const list = keys[0] //ZZZZ temporary hack
      arr = await this.#dequeueRedis.BLPOP(list, POP_TIMEOUT)
    } else {

      // Non-blocking read of numEvents elements from any of the specified queues
      console.log(`numEvents=`, numEvents)
      for (const key of keys) {
        console.log(`key=`, key)

        // NOTE: This only seems to return a single record
        const result = await this.#dequeueRedis.LPOP(key, numEvents)
        // console.log(`result=`, result)
        if (result) {
          if (Array.isArray(result)) {
            console.log(`REDIS.dequeue: Got ${result.length} from ${key}`)
            arr.push(...result)
            numEvents -= result.length
          } else {
            // Single element
            console.log(`REDIS.dequeue: Got single element from ${key}`)
            arr.push(result)
            numEvents--
          }
        }
      }
      console.log(`arr=`, arr)
      process.exit(1)

      // LMPOP would be ideal, but isn't available until REDIS v7, which isn't supported by npm module.
      // // LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
      // // See https://redis.io/commands/lmpop
      // const params = [ keys.length, ...keys, 'LEFT', numEvents ]
      // const reply = await this.#dequeueRedis.LMPOP(...params)
      // console.log(`reply=`, reply)
      // // Return value: Null, or a two-element array with the first element being the name of the
      // // key from which elements were popped, and the second element is an array of elements.
      // arr = reply ? reply[1] : []
    }

    if (!arr) {
      return [ ]
    }
    // console.log(`arr=`, arr)

    // Convert from either string or JSON
    const convertedArr = [ ]
    for (const value of arr) {
      // console.log(`  value=`, value)
      // console.log(`  value=`, typeof value)
      if (value.startsWith(STRING_PREFIX)) {
        // This is a non-JSON string
        const nval = value.substring(STRING_PREFIX.length)
        convertedArr.push(nval)
      } else {
        // JSON value
        try {
          const nval = JSON.parse(value)
          convertedArr.push(nval)
        } catch (e) {
          console.log(`Internal error: Invalid JSON pulled from event queue`)
        }
      }
    }
    // console.log(`dequeued ${convertedArr.length} item from queue`)
    return convertedArr
  }


  async queueLengths() {
    // console.log(`getQueueLengths()`)
    await this._checkLoaded()
    const keys =  await this.#adminRedis.KEYS(`${EVENT_QUEUE_PREFIX}*`)
    // console.log(`keys=`, keys)

    const queues = [ ] // [ { nodeGroup, nodeId?, queueLength }]
    if (keys) {
      for (const key of keys) {
        // datp:queue:group:GROUP or datp:queue:node:GROUP:NODEID or datp:queue:express:GROUP:NODEID
        // console.log(`key=>`, key)

        // Get the length
        const length =  await this.#adminRedis.LLEN(key)

        // Strip the prefix off the list name, to give the application's idea of the queue name.
        // group:GROUP or node:GROUP:NODEID or express:GROUP:NODEID
        const suffix = key.substring(EVENT_QUEUE_PREFIX.length)
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
      const len =  await this.#adminRedis.LLEN(list)
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
    const len =  await this.#queueRedis.LLEN(list)
    if (VERBOSE) console.log(` - ${await this.#queueRedis.LLEN(list)} before`)
    if (len > 0) {
      await this.#queueRedis.LTRIM(list, 1, 0)
      // console.log()
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      console.log(`   ${len} events in queue [${list}] have been drained, so will not be run.`.gray)
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      // console.log(``)
    }
    if (VERBOSE) console.log(` - ${await this.#queueRedis.LLEN(list)} after`)
  }

  async close() {
    if (this.#queueRedis) {
      VERBOSE && console.log(`{disconnecting from REDIS}`.gray)
      // See https://github.com/luin/ioredis/blob/master/API.md#clusterquitcallback--promise
      await this.#queueRedis.quit()
      await this.#dequeueRedis.quit()
      await this.#adminRedis.quit()
      this.#queueRedis = null
      this.#dequeueRedis = null
      this.#adminRedis = null
    }
  }

}// RedisQueue

// Work out the list name
function listName(queueName) {
  return `${EVENT_QUEUE_PREFIX}${queueName}`
}
