/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { QueueManager } from './QueueManager';
import juice from '@tooltwist/juice-client'
const Redis = require('ioredis');
const util = require('util');

// This adds colors to the String class
require('colors')

const STRING_PREFIX = 'string:::'
const POP_TIMEOUT = 0

const VERBOSE = 0

let initialRedisConnection = null

export class RedisQueue extends QueueManager {
  #redis


  constructor () {
    super()
    this.#redis = null
  }

  async _checkLoaded() {
    // console.log(`_checkLoaded`)

    // Is this connection already active?
    if (this.#redis) {
      VERBOSE && console.log(`{already connected to REDIS}`.gray)
      return
    }

    // Has another instance of this class already connected?
    if (initialRedisConnection) {
      VERBOSE && console.log(`{duplicating existing REDIS connection}`.gray)
      this.#redis = initialRedisConnection.duplicate()
      // this.#redis = initialRedisConnection
      return
    }
    VERBOSE && console.log(`{getting initial REDIS connection}`.gray)

    // console.log(`initializing REDIS`)

    const host = await juice.string('redis.host', juice.MANDATORY)
    const port = await juice.integer('redis.port', juice.MANDATORY)
    const password = await juice.string('redis.password', juice.OPTIONAL)


    if (VERBOSE) {
      console.log('----------------------------------------------------')
      console.log('Connecting to REDIS')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      }
    const options = { port, host }
    if (password) {
      options.password = password
    }
    const newRedis = new Redis(options)
    if (VERBOSE) {
      console.log('----------------------------------------------------')
    }




    if (newRedis === null) {
      console.log(`ZZZZ Bad REDIS init`)
      throw new Error('Could not initialize REDIS')
    }

    // Perhaps report all REDIS communications
    if (VERBOSE > 1) {
      newRedis.monitor(function (err, monitor) {
        // Entering monitoring mode.
        monitor.on("monitor", function (time, args, source, database) {
          console.log('   =>  ' + time + ": " + util.inspect(args));
        });
      });
    }

    newRedis.on("error", function(err) {
      console.log('----------------------------------------------------')
      console.log('An error occurred using REDIS:')
      console.log('HOST=' + host)
      console.log('PORT=' + port)
      console.log('ERROR=' + err);
    })

    initialRedisConnection = newRedis
    this.#redis = newRedis
  }

  /**
   * Add an item at the end of the queue
   * @param {QueueItem} item An object that extends QueueItem
   * @returns The item added to the queue
   */
  async enqueue(queueName, item) {
    if (VERBOSE) console.log(`RedisCache.enqueue(${queueName})`.brightBlue)
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
    const list = listName(queueName)
    VERBOSE && console.log(`{enqueue to ${list}}`.gray)
    await this.#redis.rpush(list, item)
  }

  /**
   * @returns The item at the head of the queue, removed from the queue
   */
  async dequeue(queueName, wait=true) {
    if (VERBOSE) console.log(`RedisCache.dequeue(${queueName}, ${wait})`.brightBlue)
    if (!queueName) {
      throw new Error('RedisQueue.dequeue: queueName must be specified')
    }
    await this._checkLoaded()
    const list = listName(queueName)
    // console.log(`dequeue from ${list}`)
    let value = null
    if (wait) {
      const arr = await this.#redis.blpop(list, POP_TIMEOUT)
      if (arr) {
        // arr is [ listname, value ]
        value = arr[1]
      }
    } else {
      value = await this.#redis.lpop(list)
    }

    // console.log(`  value=`, value)
    // console.log(`  value=`, typeof value)
    if (value) {
      if (value.startsWith(STRING_PREFIX)) {
        value = value.substring(STRING_PREFIX.length)
      } else {
        value = JSON.parse(value)
      }
      // console.log(`{dequeued from ${list}}`, value)
      VERBOSE && console.log(`{dequeued from ${list}}`.gray)
      return value
    }
    return null
  }


   async queueLength(queueName) {
    if (!queueName) {
      throw new Error('RedisQueue.queueLength: queueName must be specified')
    }
    try {
      await this._checkLoaded()
      const list = listName(queueName)
      const len =  await this.#redis.llen(list)
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
    const len =  await this.#redis.llen(list)
    if (VERBOSE) console.log(` - ${await this.#redis.llen(list)} before`)
    if (len > 0) {
      await this.#redis.ltrim(list, 1, 0)
      // console.log()
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      console.log(`   ${len} events in queue [${list}] have been drained, so will not be run.`.gray)
      // console.log(`   WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING  WARNING`)
      // console.log(``)
    }
    if (VERBOSE) console.log(` - ${await this.#redis.llen(list)} after`)
  }

  async close() {
    if (this.#redis) {
      VERBOSE && console.log(`{disconnecting from REDIS}`.gray)
      // See https://github.com/luin/ioredis/blob/master/API.md#clusterquitcallback--promise
      await this.#redis.quit()
      this.#redis = null
    }
  }

}// RedisQueue

// Work out the list name
function listName(queueName) {
  return `datp:queue:${queueName}`
}
