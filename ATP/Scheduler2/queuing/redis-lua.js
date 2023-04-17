/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import juice from '@tooltwist/juice-client'
import assert from 'assert';
import query from '../../../database/query';
import { deepCopy } from '../../../lib/deepCopy';
import pause from '../../../lib/pause';
import { DEFINITION_STEP_COMPLETE_EVENT, DEFINITION_PROCESS_STEP_START_EVENT, STEP_DEFINITION, validateStandardObject } from '../eventValidation';
import TransactionState from '../TransactionState';
import Transaction from '../TransactionState';
import { registerLuaScripts_cachedState } from './redis-cachedState';
import { registerLuaScripts_dequeue } from './redis-dequeue';
import { registerLuaScripts_endPipeline } from './redis-endPipeline';
import { registerLuaScripts_exceptions } from './redis-exceptions';
import { registerLuaScripts_metrics } from './redis-metrics';
import { registerLuaScripts_sleep } from './redis-retry';
import { registerLuaScripts_startStep } from './redis-startStep';
import { registerLuaScripts_transactions } from './redis-transactions';
import { registerLuaScripts_txComplete } from './redis-txCompleted';
import { registerLuaScripts_webhooks } from './redis-webhooks';
const Redis = require('ioredis');

export const VERBOSE = 0
export const SHOWLOG = 0

export const FLOW_VERBOSE = 0
export const FLOW_PARANOID = 0

// Only get the connection once
const CONNECTION_NONE = 0
const CONNECTION_WAIT = 1
const CONNECTION_READY = 2
let connectionStatus = CONNECTION_NONE

// Connections to REDIS
let connection_enqueue = null
let connection_admin = null

/*
 *  Externalizing State Fields
 *  --------------------------
 *  Some of the fields in the transation state are needed by these Lua
 *  functions. To avoid Lua parsing JSON, we extract the values from
 *  JSON and pass them in, alongside the JSON.
 * 
 *  The big question then... can we then delete them from the JSON and
 *  save space, and remove the possibility of inconsistency?
 */
export const EXTERNALIZE_BUT_DONT_DELETE = 0
export const EXTERNALIZE_FROM_COPY_AND_DELETE = 1 // Why doesn't this work?
export const externalizationMode = EXTERNALIZE_BUT_DONT_DELETE


/*
 *  Node registration.
 */
//ZZZZ Unexport this and move related functionality that gets broken here
export const KEYSPACE_NODE_REGISTRATION = 'datp:node-registration:RECORD:'

/**
 * Event queues.
 * One of each per nodeGroup.
 * LIST
 */
export const KEYSPACE_QUEUE_IN = 'qInL:'
export const KEYSPACE_QUEUE_OUT = 'qOutL:'
export const KEYSPACE_QUEUE_ADMIN = 'qAdminL:'

/**
 * Transactions currently being processed.
 *    txId => startTime
 */
export const KEYSPACE_PROCESSING = 'processingZ'

/**
 * Transaction state.
 * One per transaction.
 * HASH
 */
export const KEYSPACE_STATE = 'stateH:'

export const KEYSPACE_EXTERNALID = 'externalId:'
export const EXTERNALID_UNIQUENESS_PERIOD = 6 * 60 * 60 // An externalId may nto be reused for this many seconds


/**
 * Transaction state.
 * One per transaction.
 * HASH
 */
export const KEYSPACE_STATS_1 = 'metric1H:'
// const STATS_INTERVAL_1 = 15 // 15 seconds
export const STATS_INTERVAL_1 = 5
// const STATS_RETENTION_1 = 15 * 60 // 15 minutes
export const STATS_RETENTION_1 = 5 * 60
export const KEYSPACE_STATS_2 = 'metric2H:'
// const STATS_INTERVAL_2 = 5 * 60 // 5 minutes
export const STATS_INTERVAL_2 = 60
export const STATS_RETENTION_2 = 2 * 60 * 60 // 2 hours
export const KEYSPACE_STATS_3 = 'metric3H:'
// const STATS_INTERVAL_3 = 60 * 60 // 1 hour
export const STATS_INTERVAL_3 = 10 * 60
export const STATS_RETENTION_3 = 24 * 60 * 60 // 24 hours

/**
 * Transactions that are sleeping.
 *    txId => wake time.
 * - The transaction state may be archived by the wake time.
 */
export const KEYSPACE_SLEEPING = 'sleepingZ'
export const KEYSPACE_WAKEUP_PROCESSING_LOCK = 'wakeupLock'
export const MIN_WAKEUP_PROCESSING_INTERVAL = 10 // seconds
/**
 * Transactions that are in a broken state, outside the normal processing flow.
 *    txId => time of being orphaned
 */
export const KEYSPACE_EXCEPTION = 'exceptionZ'

/**
 * IDs of transactions that can be archived to long term storage.
 *  txId => timeToArchive
 *  - A list of txIds that should be persisted to long term storage.
 *  - The transaction state remains in the State table.
 *  - Can be sleeping, or complete transactions.
 *  - A dedicated process saves to DB, then removes from State and toArchive.
 */
export const KEYSPACE_TOARCHIVE = 'toArchiveZ'
export const KEYSPACE_TOARCHIVE_LOCK1 = 'toArchiveLock1'
export const KEYSPACE_TOARCHIVE_LOCK2 = 'toArchiveLock2'

export const PERSISTING_PERMISSION_LOCK1_PERIOD = 60 // A node is given exclusive permission to persist transaction for this many seconds
export const PERSISTING_PERMISSION_LOCK2_PERIOD = PERSISTING_PERMISSION_LOCK1_PERIOD + 5 //ZZ 30
export const DELAY_BEFORE_ARCHIVING = 1 //ZZZ 20 Minutes of delay before archiving and removing from REDIS (gives time for MONDAT to look at the transaction)

/**
 * IDs of completed transactions that need a reply via webhook.
 *    txId => retryTime
 * The time of the next webhook attempt is used as a sort key (initially zero, with an exponential backoff).
 * - At the time of accessing a record, it gets rescheduled at the next retry time.
 * - If a webhook attempt is successful we delete the txID from the list so there is no retry.
 * - Retry time has an exponential back-off of (30 * 1.8^retryNumber). e.g. 30, 54, 97, 175, 152, 315, etc
 */
export const KEYSPACE_SCHEDULED_WEBHOOK = 'webhooksZ'
export const KEYSPACE_WEBHOOK_FAIL_COUNTER = 'webhookFail:'
export const KEYSPACE_WEBHOOK_FAIL_LIMIT = 10
export const KEYSPACE_WEBHOOK_FAIL_PERIOD = 2 * 60
// export const WEBHOOK_INITIAL_DELAY = 30 * 1000
// export const WEBHOOK_EXPONENTIAL_BACKOFF = 1.8
export const WEBHOOK_INITIAL_DELAY = 15 * 1000
export const WEBHOOK_EXPONENTIAL_BACKOFF = 1
export const MAX_WEBHOOK_RETRIES = 8

/**
 * Pipelne definitions
 */
export const KEYSPACE_PIPELINE = 'pipelineH:'

/**
 * pub/sub channel for completed transactions and progress reports.
 */
export const CHANNEL_NOTIFY = 'datp-notify'


/*
 *  Mapping between values in the txState and the hash fields when it is stored in REDIS.
 */
export const STATE_TO_REDIS_EXTERNALIZATION_MAPPING = [
  { txState: 'transactionData.transactionType',   redis: 'transactionType' },
  { txState: 'transactionData.status',            redis: 'status' },
  { txState: 'transactionData.metadata.webhook',  redis: 'webhook' },
  { txState: 'transactionData.startTime',         redis: 'startTime', type: 'number' },
  { txState: 'transactionData.completionTime',    redis: 'completionTime', type: 'number' },
  { txState: 'transactionData.completionNote',    redis: 'completionNote' },
  { txState: 'transactionData.lastUpdated',       redis: 'lastUpdated', type: 'number' },
  { txState: 'transactionData.notifiedTime',      redis: 'notifiedTime', type: 'number' },
  { txState: 'webhook.type',                      redis: 'webhook_type' },
  { txState: 'webhook.retryCount',                redis: 'webhook_retryCount', type: 'number' },
  { txState: 'webhook.status',                    redis: 'webhook_status' },
  { txState: 'webhook.comment',                   redis: 'webhook_comment' },
  { txState: 'webhook.initialAttempt',            redis: 'webhook_initial' },
  { txState: 'webhook.latestAttempt',             redis: 'webhook_latest' },
  { txState: 'webhook.nextAttempt',               redis: 'webhook_next' },
  { txState: 'transactionData.yarp',              redis: 'yarp' }, // Spare
]
export const NUMBER_OF_EXTERNALIZED_FIELDS = STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length

export const STATE_TO_REDIS_READONLY_MAPPING = [
  { txState: 'txId',                              redis: 'txId' },
  { txState: 'owner',                             redis: 'owner' },
  { txState: 'externalId',                        redis: 'externalId' },
  { txState: 'retry.sleepingSince',               redis: 'retry_sleepingSince', type: 'number' },
  { txState: 'retry.sleepCounter',                redis: 'retry_counter', type: 'number' },
  { txState: 'retry.wakeTime',                    redis: 'retry_wakeTime', type: 'number' },
  { txState: 'retry.wakeSwitch',                  redis: 'retry_wakeSwitch' },
  // Note: we should not access the switches from a TransactionState
  // object. This is only here so we can archive the state, and for display in MONDAT.
  { txState: 'transactionData.switches',          redis: 'switches' },
]
export const NUMBER_OF_READONLY_FIELDS = STATE_TO_REDIS_READONLY_MAPPING.length


export const PROCESSING_STATE_QUEUED = 'queued'
export const PROCESSING_STATE_PROCESSING = 'processing'
export const PROCESSING_STATE_SLEEPING = 'sleeping'
export const PROCESSING_STATE_COMPLETE = 'complete'
export const PROCESSING_STATE_CANCELLED = 'cancelled'


export class RedisLua {

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

    await newRedis.connect()
    await newRedis2.connect()

    connection_enqueue = newRedis
    connection_admin = newRedis2

    // Ready for action!
    if (VERBOSE>1) console.log(`{REDIS connection ready}`.gray)
    connectionStatus = CONNECTION_READY

    // Prepare the LUA scripts
    await registerLuaScripts_transactions()
    await registerLuaScripts_startStep()
    await registerLuaScripts_endPipeline()
    await registerLuaScripts_dequeue()
    await registerLuaScripts_metrics()
    await registerLuaScripts_cachedState()
    await registerLuaScripts_webhooks()
    await registerLuaScripts_txComplete()
    await registerLuaScripts_sleep()
    await registerLuaScripts_exceptions()
  }//- _checkLoaded

  static async regularConnection() {
    await RedisLua._checkLoaded()
    return connection_enqueue
  }

  static async adminConnection() {
    await RedisLua._checkLoaded()
    return connection_admin
  }


  static async registerLuaScripts() {


    /*
     *  Add an event to a queue.
     *  1. If externalId is provided, check it is not already used.
     *  2. Check this transaction does not already have queued status.
     *  3. If pipeline is provided, get pipeline definition and nodeGroup.
     *  4. Determine the queue key.
     *  5. Save the transaction state.
     *  6. Save the event.
     *  7. Remove it from the 'processing' list
     *  8. Update statistics
     * 
     * Operation will be one of:
     *    start-transaction - start a new transaction (check externalId)
     *    start-pipeline - start a pipeline ('pipeline' provides the pipeline and nodeGroup)
     *    sleep - prepare for step retry and set 'wakeTime' in sleeping list
     *    end-pipeline - end of pipeline
     */
    // See Learn Lua in 15 minutes (https://tylerneylon.com/a/learn-lua/)
    connection_enqueue.defineCommand("datp_enqueue", {
      numberOfKeys: 3,
      lua: `
        local pipelineKey = KEYS[1]
        local stateKey = KEYS[2]
        local externalIdKey = KEYS[3]

        -- arguments to this function
        local operation = ARGV[1]
        local queueType = ARGV[2]
        local pipeline = ARGV[3]
        local nodeGroup = ARGV[4] -- Overridden if we have a pipeline.
        local eventJSON = ARGV[5]
        local stateJSON = ARGV[6]
        local txOutputJSON = ARGV[7]
        local owner = ARGV[8]
        local txId = ARGV[9]
        local webhook = ARGV[10]
        local eventType = ARGV[11]
        local isInitialTransaction = ARGV[12] -- used to update stats

        -- variables
        local INDEX_OF_EXTERNALIZED_FIELDS = 13
        local log = { }

        -- 1. If externalId is provided
        -- 1a. Check it is not already used
        -- 1b. Remember the externalId
        if externalIdKey ~= '' then
          -- Check it is not already set
          local mappedTxId = redis.call('get', externalIdKey)
          if mappedTxId then
            return { 'duplicate', 'External ID is already used [' .. externalIdKey .. ']', mappedTxId  }
          end

          -- Save the mapping and add an expiry time
          redis.call('set', externalIdKey, txId)
          redis.call('expire', externalIdKey, ${EXTERNALID_UNIQUENESS_PERIOD})
          log[#log+1] = 'Checked externalId is not already used'
        end

        -- 2. Check this transaction does not already have queued status
        local processingState = redis.call('hget', stateKey, 'processingState')
        if processingState == '${PROCESSING_STATE_QUEUED}' then
          local msg = 'Internal Error: datp_enqueue: Transaction is already queued'
          return { 'error', 'E99288', txId, msg }
        end
        log[#log+1] = 'Checked transaction does not already have queued status'

        -- 3. If a pipeline is specified, get the nodeGroup from the pipeline definition.
        local pipelineVersion = nil
        if pipeline ~= '' then  
          local details = redis.call('hmget', pipelineKey,
              'version',
              'nodeGroup')
          pipelineVersion = details[1]
          nodeGroup = details[2]
          if pipelineVersion == nil then
            local msg = 'Internal Error: datp_enqueue: Unknown pipeline [' .. pipeline .. ']'
            return { 'error', 'E982662', pipeline, msg }
          end
          log[#log+1] = 'Got pipeline definition'

        end

        -- 4. Determine the queue key
        local queueKey
        if queueType == 'in' then
          queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
        elseif queueType == 'out' then
          queueKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
        elseif queueType == 'admin' then
          queueKey = '${KEYSPACE_QUEUE_ADMIN}' .. nodeGroup
        end
        log[#log+1] = 'Determined that pipeline uses queue ' .. queueKey

        -- Get the current time
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        log[#log+1] = 'Current time is ' .. now

        -- 5. Save the transaction state
        redis.call('hset', stateKey,
          'txId', txId,
          'processingState', '${PROCESSING_STATE_QUEUED}',
          'event', eventJSON,
          'state', stateJSON,
          'txOutput', txOutputJSON,
          'eventType', eventType,
          'nodeGroup', nodeGroup,
          'pipeline', pipeline,
          'ts', now,
          'queue', queueKey,
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[0].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+0],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[1].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+1],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[2].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+2],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[3].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+3],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[4].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+4],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[5].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+5],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[6].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+6],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[7].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+7],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[8].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+8],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[9].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+9],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[10].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+10],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[11].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+11],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[12].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+12],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[13].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+13],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[14].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+14],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[14].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+15]
        )
        if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 16 then
          return { 'error', 'E9626322', 'datp_enqueue', 'Incorrect number of parameters' }
        end
        log[#log+1] = 'Saved the transaction state as JSON and externalized fields'

        -- 6. Save the event
        if eventJSON ~= '' then
          redis.call('rpush', queueKey, txId)
          log[#log+1] = 'Added the event to the queue'
        end
        
        -- Save the webhook, if there is one.
        --if webhook ~= '' then
        --  local stateKey = '${KEYSPACE_STATE}' .. txId
        --  redis.call('hset', stateKey, 'webhook', webhook)
        --end

        -- 7. Remove it from the 'processing' list
        redis.call('zrem', '${KEYSPACE_PROCESSING}', txId)
        log[#log+1] = 'Remove the transaction ID from the "processing list"'


        -- 8. Update statistics
        -- (work out the keys first)
        local qInKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
        local qOutKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
        local inputLength = redis.call('llen', qInKey)
        local outputLength = redis.call('llen', qOutKey)
        local f_start = 'queued'
        local f_txStart = 'tx:::queued'
        local f_nodeGroup = 'g:::' .. nodeGroup .. ':::queued'
        local f_pipelineStart = 'p:::' .. pipeline .. ':::queued'
        local f_txPipelineStart = 'tx:::' .. pipeline .. ':::queued'
        local f_ownerStart = 'o:::' .. owner .. ':::queued'
        local f_ownerPipeline = 'o:::' .. owner .. ':::p:::' .. pipeline .. ':::queued'
        local f_qInLength = 'iLen:::' .. nodeGroup
        local f_qOutLength = 'oLen:::' .. nodeGroup

        -- Short interval
        local timeslot1 = math.floor(seconds / ${STATS_INTERVAL_1}) * ${STATS_INTERVAL_1}
        local expiry1 = timeslot1 + ${STATS_RETENTION_1}
        local statsKey1 = '${KEYSPACE_STATS_1}' .. timeslot1
        redis.call('hincrby', statsKey1, f_start, 1)
        redis.call('hincrby', statsKey1, f_ownerStart, 1)
        redis.call('hincrby', statsKey1, f_nodeGroup, 1)
        if isInitialTransaction == 'true' then
          redis.call('hincrby', statsKey1, f_txStart, 1)
        end
        if pipeline ~= '' then
          redis.call('hincrby', statsKey1, f_pipelineStart, 1)
          redis.call('hincrby', statsKey1, f_ownerPipeline, 1)
          if isInitialTransaction == 'true' then
            redis.call('hincrby', statsKey1, f_txPipelineStart, 1)
          end
        end
        redis.call('hset', statsKey1, f_qInLength, inputLength)
        redis.call('hset', statsKey1, f_qOutLength, outputLength)
        redis.call('expireat', statsKey1, expiry1)

        -- Medium interval
        local timeslot2 = math.floor(seconds / ${STATS_INTERVAL_2}) * ${STATS_INTERVAL_2}
        local expiry2 = timeslot2 + ${STATS_RETENTION_2}
        local statsKey2 = '${KEYSPACE_STATS_2}' .. timeslot2
        redis.call('hincrby', statsKey2, f_start, 1)
        redis.call('hincrby', statsKey2, f_ownerStart, 1)
        redis.call('hincrby', statsKey2, f_nodeGroup, 1)
        if isInitialTransaction == 'true' then
          redis.call('hincrby', statsKey2, f_txStart, 1)
        end
        if pipeline ~= '' then
          redis.call('hincrby', statsKey2, f_pipelineStart, 1)
          redis.call('hincrby', statsKey2, f_ownerPipeline, 1)
          if isInitialTransaction == 'true' then
            redis.call('hincrby', statsKey2, f_txPipelineStart, 1)
          end
        end
        redis.call('hset', statsKey2, f_qInLength, inputLength)
        redis.call('hset', statsKey2, f_qOutLength, outputLength)
        redis.call('expireat', statsKey2, expiry2)

        -- Long interval
        local timeslot3 = math.floor(seconds / ${STATS_INTERVAL_3}) * ${STATS_INTERVAL_3}
        local expiry3 = timeslot3 + ${STATS_RETENTION_3}
        local statsKey3 = '${KEYSPACE_STATS_3}' .. timeslot3
        redis.call('set', '@start', timeslot3)
        redis.call('set', '@expire', expiry3)
        redis.call('hincrby', statsKey3, f_start, 1)
        redis.call('hincrby', statsKey3, f_ownerStart, 1)
        redis.call('hincrby', statsKey3, f_nodeGroup, 1)
        if isInitialTransaction == 'true' then
          redis.call('hincrby', statsKey3, f_txStart, 1)
        end
        if pipeline ~= '' then
          redis.call('hincrby', statsKey3, f_pipelineStart, 1)
          redis.call('hincrby', statsKey3, f_ownerPipeline, 1)
          if isInitialTransaction == 'true' then
            redis.call('hincrby', statsKey3, f_txPipelineStart, 1)
          end
        end
        redis.call('hset', statsKey3, f_qInLength, inputLength)
        redis.call('hset', statsKey3, f_qOutLength, outputLength)
        redis.call('expireat', statsKey3, expiry3)
        log[#log+1] = 'Updated the metrics collection'
        
        return { 'ok', queueKey, nodeGroup, pipeline, pipelineVersion, webhook, log }
      `
    });


}//- registerLuaScripts


  // /**
  //  * Put a transaction to sleep, waiting for either a prescribed amount of time, 
  //  * of for a switch to be set.
  //  * 
  //  * @param {*} txId 
  //  * @param {*} nameOfSwitch 
  //  * @param {*} sleepDuration 
  //  */
  // async luaInitiateSleep(txId, nodeGroup, f2i, nameOfSwitch, sleepDuration) {
  //   if (FLOW_VERBOSE) console.log(`----- luaInitiateSleep(txId=${txId}, f2i=${f2i}, sleepDuration=${sleepDuration})`.yellow)
  //   let result
  //   try {
  //     const stateKey = `${KEYSPACE_STATE}${txId}`
  //     result = await connection_enqueue.datp_initiateSleep(
  //       // Keys
  //       stateKey,
  //       // Parameters
  //       txId,
  //       nodeGroup,
  //       f2i,
  //       nameOfSwitch,
  //       sleepDuration,
  //     )
  //     console.log(`datp_initiateSleep returned`.red, result)

  //     switch (result[0]) {
  //       case 'error':
  //         const msg = `${result[1]}: ${result[2]}: ${result[3]}`
  //         console.log(msg)
  //         throw new Error(msg)
  
  //       case 'ok':
  //         const log = result[1]
  //         if (SHOWLOG) console.log(`datp_initiateSleep =>`, log)
  //         const reply = { }
  //         return reply
  //     }
  //   } catch (e) {
  //     console.log(`FATAL ERROR calling LUA script [datp_initiateSleep]`)
  //     throw e
  //   }
  // }//- luaInitiateSleep


//   /**
//    * Add an event to a queue.
//    * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
//    * 
//    * If pipeline is specified, the pipeline definition is used to determine the nodegroup,
//    * and the pipeline definition will be appended to the reply when the event is dequeued.
//    * 
//    * @param {string} queueType in | out | admin
//    * @param {string} pipeline If provided, we'll get the nodeGroup from the pipelne
//    * @param {string} nodeGroup
//    * @param {object} event
//    */
//   //ZZZZZ Not implemented yet.
//    async luaEnqueue_continue(queueType, txState, event, pipeline, checkExternalIdIsUnique, nodeGroup=null) {
//     if (FLOW_VERBOSE) console.log(`----- luaEnqueue_continue(queueType=${queueType}, txState, event, pipeline=${pipeline}, checkExternalIdIsUnique=${checkExternalIdIsUnique}, nodeGroup=${nodeGroup})`.yellow)
//     // console.log(`txState=${txState.pretty()}`.magenta)

//     let externalIdKey = ''
//     if (checkExternalIdIsUnique) {
//       // console.log(`NEED TO CHECK EXTERNALID IS UNIQUE`.magenta)
//       const owner = txState.getOwner()
//       const externalId = txState.getExternalId()
//       externalIdKey = `${KEYSPACE_EXTERNALID}${owner}:::${externalId}` // If set, we check it's unique and save the mapping
//       // console.log(`externalIdKey=`, externalIdKey)
//     }

//     if (FLOW_PARANOID) {
//       switch (queueType) {
//         case 'in':
//           validateStandardObject('luaEnqueue_continue() event', event, DEFINITION_PROCESS_STEP_START_EVENT)
//           break

//         case 'out':
//           validateStandardObject('luaEnqueue_continue() event', event, DEFINITION_STEP_COMPLETE_EVENT)
//           break
//       }
//       assert(txState instanceof Transaction)
//       txState.vog_validateSteps()
//     }

//     // Must specify either pipeline or a nodeFroup
//     assert(pipeline || nodeGroup)
//     assert( !(pipeline && nodeGroup))

//     await RedisLua._checkLoaded()

//     const eventType = event.eventType

//     const txId = txState.getTxId()
//     const owner = txState.getOwner()
//     // txState.vog_setStatusToQueued()

//     const metadata = txState.vog_getMetadata()
//     const webhook = (metadata.webhook) ? metadata.webhook : ''

//     // Save the txState and the event, and perhaps the webhook
//     let queueKey
//     switch (queueType) {
//       case 'in':
//         queueKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`
//         break
//       case 'out':
//         queueKey = `${KEYSPACE_QUEUE_OUT}${nodeGroup}`
//         break
//       case 'admin':
//         queueKey = `${KEYSPACE_QUEUE_ADMIN}${nodeGroup}`
//         break
//     }

// // console.log(`event=`.magenta, event)
//     const tmpTxState = event.txState
//     delete event.txState //ZZZ Should not be set
//     const eventJSON = JSON.stringify(event)
//     event.txState = tmpTxState
// // console.log(`eventJSON=`.magenta, eventJSON)

//     // Is this the initial transaction?
//     const txStateObject = txState.asObject()
//     const isInitialTransaction = (txStateObject.f2.length <= 3)
//     // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


//     const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
//     const stateKey = `${KEYSPACE_STATE}${txId}`
//     const runningKey = `${KEYSPACE_PROCESSING}`

//     const transactionOutputJSON = txState.getTransactionOutputAsJSON()

//     const { stateJSON, externalisedStateFields } = externalizeTransactionState(txState)
//     // console.log(`stateJSON=`, stateJSON)
//     // console.log(`externalisedStateFields=`, externalisedStateFields)

//     // // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
//     // const externalisedStateFields = [ ]
//     // let txStateObject = txState.asObject()

//     // // Extract the externalized fields from the transaction state.
//     // // If we delete them then the stateJSON is a little bit smaller, but we
//     // // have a problem because other code might still be using this state object.
//     // // So, we have three options:

//     // // Option 1. Don't delete the externalized values.
//     // // const PUT_VALUES_BACK = false
//     // // const doDelete = false

//     // // Option 2. Use a copy of the transaction state.
//     // const PUT_VALUES_BACK = false
//     // txStateObject = deepCopy(txStateObject)

//     // // Option 3. Put the values back later
//     // // const PUT_VALUES_BACK = true
//     // // const doDelete = true

//     // for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
//     //   const value = externaliseStateField_extractValue(txStateObject, map.txState)
//     //   // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
//     //   externalisedStateFields.push(value)
//     // }


//     // console.log(`externalisedStateFields=`, externalisedStateFields)
//     let result
//     try {
//       console.log(`Calling datp_enqueue from luaEnqueue_continue`)
//       result = await connection_enqueue.datp_enqueue(
//           // Keys
//           pipelineKey, stateKey, externalIdKey,
//           // Parameters
//           'sleep',
//           queueType, pipeline, nodeGroup,
//           eventJSON, stateJSON, transactionOutputJSON,
//           owner, txId, webhook,
//           eventType,
//           isInitialTransaction ? 'true' : 'false',
//           externalisedStateFields)
//       // console.log(`datp_enqueue returned`, result)

//       switch (result[0]) {
//         case 'duplicate':
//           throw new Error(`CANNOT CREATE A DUPLICATE TRANSACTION`)
  
//         case 'error':
//           const msg = `${result[1]}: ${result[2]}: ${result[3]}`
//           console.log(msg)
//           throw new Error(msg)
  
//         case 'ok':
//           const log = result[6]
//           if (SHOWLOG) console.log(`datp_enqueue =>`, log)
//           const reply = {
//             queue: result[1],
//             nodeGroup: result[2],
//             pipelineName: result[3],
//             pipelineVersion: result[4],
//             webhook: result[5] ? result[5] : null
//           }

//           // Put the extracted values back in (if required)
//           de_externalizeTransactionState(txState, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, externalisedStateFields, 0)
//           // console.log(`State afterwards=${txState.stringify()}`)
//           return reply
//       }
//     } catch (e) {
//       console.log(`FATAL ERROR calling LUA script [datp_enqueue]`)
//       throw e
//     }
//   }//- luaEnqueue_continue

//   /**
//    * Get the state of a transaction.
//    * 
//    * @param {string} txId 
//    * @returns 
//    */
//   async getCachedState(txId, withMondatDetails=false, markAsReplied=false, cancelWebhook=false) {
//     if (VERBOSE) console.log(`----- getCachedState(${txId}, withMondatDetails=${withMondatDetails}, markAsReplied=${markAsReplied}, cancelWebhook=${cancelWebhook}))`.yellow)
//     await RedisLua._checkLoaded()
//     const stateKey = `${KEYSPACE_STATE}${txId}`
//     const webhookKey = KEYSPACE_SCHEDULED_WEBHOOK

//     let result
//     try {
//       result = await connection_enqueue.datp_getCachedState(stateKey, txId, withMondatDetails, markAsReplied, cancelWebhook)
//       // if (result.length > 0 && result[0] !== null) {
//       //   console.log(`luaGetCachedState(): result=`, result)
//       // }
//     } catch (e) {
//       console.log(`FATAL ERROR calling LUA script [datp_getCachedState]`)
//       throw e
//     }


//     const txId2 = result[0]
//     const processingState = result[1]
//     const eventJSON = result[2]
//     const stateJSON = result[3]
//     const nodeGroup = result[4]
//     const pipeline = result[5]
//     const queue = result[6]
//     let ts = parseInt(result[7])
//     // const queue = result[7]
//     const firstExternalizedFieldIndex = 8
//     // const txState = new TransactionState(stateJSON)
//     // // const txStateObject = JSON.parse(stateJSON)
//     // const txStateObject = txState.asObject()
//     // // console.log(`txStateObject BEFORE =`.brightRed, txStateObject)
//     // for (const [i, map] of STATE_TO_REDIS_EXTERNALIZATION_MAPPING.entries()) {
//     //   // console.log(`${map.txState} <== ${i} ${map.redis} = ${result[4 + i]}    (${typeof(result[4 + i])})`)
//     //   const value = result[firstExternalizedFieldIndex + i]
//     //   externaliseStateField_setValue(txStateObject, map.txState, value)
//     // }

//     // console.log(`txStateObject AFTER =`.brightRed, txStateObject)
//     let inProcessingList = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 0]
//     let inSleepingList = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 1]
//     let inWebhookList = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 2]
//     let toArchive = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 3]
//     let ttl = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 4]
//     // const log = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length + 5]
//     // if (SHOWLOG) console.log(`datp_getCachedState =>`, log)
// // console.log(`inProcessingList=`, inProcessingList)
// // console.log(`inSleepingList=`, inSleepingList)

//     inProcessingList = parseInt(inProcessingList)
//     toArchive = parseInt(toArchive)
//     inWebhookList = parseInt(inWebhookList)
//     inSleepingList = parseInt(inSleepingList)
//     ttl = parseInt(ttl)
//     ts = parseInt(ts)


//     // if (txId2 === null) {
//     //   // Not found
//     //   console.log(`Transaction state not cached - checking the archives (${txId})`)
//     //   // const state = await getArchivedTransactionState(txId)
//     //   // if (state === null) {
//     //   //   console.trace(`Transaction state not cached (txId=${txId})`)
//     //   // }
//     //   return null
//     // }
//     if (VERBOSE) {
//       console.log(`Found transaction state in the cache`)
//     }
//     if (txId2 === null) {
//       // Not in the cache
//       return {
//         cached: false,
//         processingState,
//         inProcessingList,
//         toArchive,
//         inWebhookList,
//         inSleepingList,
//         nodeGroup,
//         pipeline,
//         queue,
//         ttl,
//         ts,
//       }
//     }

//     // console.log(`stateJSON=`, stateJSON)
//     // console.log(`processingState=`, processingState)


//     const txState = transactionStateFromJsonAndExternalizedFields(stateJSON, result, firstExternalizedFieldIndex)
//     // console.log(`\n\ntxState=`.red, txState.asObject())
//     const reply = {
//       cached: true,
//       txId,
//       txState,
//       processingState,
//       inProcessingList,
//       toArchive,
//       inWebhookList,
//       inSleepingList,
//       nodeGroup,
//       pipeline,
//       queue,
//       ttl,
//       ts,
//       // queue
//     }
//     if (eventJSON) {
//       const event = eventJSON ? JSON.parse(eventJSON) : null
//       reply.event = event
//     }

//     // console.log(`WIBBLE reply=`, reply)

//     return reply
//   }//- getCachedState


  async keys(pattern) {
    await RedisLua._checkLoaded()
    return await connection_enqueue.keys(pattern)
  }

  // /**
  //  * Update the status of a transaction that will proceed no further.
  //  * 
  //  * @param {string} txId 
  //  * @param {string} status success | failed | aborted | internal-error
  //  * @returns 
  //  */
  // async luaTransactionCompleted(tx) {
  //   // if (VERBOSE) console.log(`luaTransactionCompleted(${tx.getTxId()})`)
  //   assert (typeof(tx) === 'object')

  //   const txId = tx.getTxId()
  //   const status = tx.getStatus()
  //   const metadata = tx.vog_getMetadata()
  //   const webhook = tx.vog_getWebhook()
  //   if (FLOW_VERBOSE) flow2Msg(tx, `----- luaTransactionCompleted(${txId}, ${status}, ${webhook})`)
  //   // console.log(`tx.pretty()=`, tx.pretty())

  //   if (status !== 'success' && status!=='failed' && status!=='aborted' && status!=='internal-error') {
  //     throw new Error('Invalid completion status')
  //   }

  //   // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
  //   // See README-txState-in-REDIS.md
  //   const externalisedStateFields = [ ]
  //   const txStateObject = tx.asObject()
  //   const doDelete = false
  //   for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
  //     const value = externaliseStateField_extractValue(txStateObject, map.txState, doDelete)
  //     // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
  //     externalisedStateFields.push(value)
  //   }


  //   const stateJSON = tx.stringify()
  //   assert(txId)
  //   assert(status)
  //   assert(stateJSON)

  //   // Call the LUA script
  //   await RedisLua._checkLoaded()
  //   const stateKey = `${KEYSPACE_STATE}${txId}`
  //   const webhookKey = requiresWebhookReply(metadata) ? `${KEYSPACE_SCHEDULED_WEBHOOK}` : null
  //   const runningKey = `${KEYSPACE_PROCESSING}`
  //   const persistKey = `${KEYSPACE_TOARCHIVE}`

  //   // console.log(`webhookKey=`, webhookKey)
  //   try {
  //     const result = await connection_enqueue.datp_completeTransaction(stateKey, webhookKey, runningKey, persistKey, txId, status, stateJSON, externalisedStateFields)
  //     const log = result[3]
  //     if (SHOWLOG) console.log(`datp_completeTransaction =>`, log)
  //     return true
  //   } catch (e) {
  //     console.log(`FATAL ERROR calling LUA script [datp_completeTransaction]`)
  //     throw e
  //   }
  // }

  async deleteZZZ(txId) {
    console.log(`deleteZZZ(${txId})`)
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const result = await connection_enqueue.hdel(stateKey, 'txId', 'processingState', 'ts', 'state', 'event', 'pipeline', 'eventType', 'queue')
    // console.log(`result=`, result)
  }



  
  // /**
  //  * Get a list of transaction states that need to be persisted.
  //  * After the details have been persisted, this function should be called again with
  //  * 'persistedTransactionIds' so the transactions are removed from the list of
  //  *  transactions needing to be archived.
  //  * 
  //  * @param {Array<string>} persistedTransactionIds IDs of transactions that have been persisted
  //  * @param {string} nodeId ID of node offerring to persist transactions
  //  * @param {Integer} numRequired Number of transaction IDs to persist
  //  * @returns Array of transaction details
  //  */
  // async transactionsToArchive(persistedTransactionIds, nodeId, numRequired) {
  //   // console.log(`----- transactionsToArchive(persistedTransactionIds, ${nodeId}, ${numRequired})`, persistedTransactionIds)
  //   await RedisLua._checkLoaded()
  //   try {
  //     const result = await connection_enqueue.datp_transactionsToArchive(nodeId, numRequired, persistedTransactionIds.length, persistedTransactionIds)
  //     // if (result.length > 0) {
  //     //   console.log(`datp_transactionsToArchive result=`.magenta, result)
  //     //   console.log(`datp_transactionsToArchive result=`.magenta, result.length)
  //     // }
  //     return result
  //   } catch (e) {
  //     console.log(`FATAL ERROR calling LUA script [datp_transactionsToArchive]`)
  //     throw e
  //   }
  // }
  

//   /**
//    * Get a list of transaction states that need to be persisted.
//    * After the details have been persisted, this function should be called again with
//    * 'persistedTransactionIds' so the transactions are removed from the list of
//    *  transactions needing to be archived.
//    * 
//    * @param {Array<string>} persistedTransactionIds IDs of transactions that have been persisted
//    * @param {string} nodeId ID of node offerring to persist transactions
//    * @param {Integer} numRequired Number of transaction IDs to persist
//    * @returns Array of transaction details
//    */
//   async getWebhooksToProcess(webhookResults, numRequired) {
//     if (VERBOSE && webhookResults.length > 0) {
//       console.log(`----- getWebhooksToProcess(webhookResults, ${numRequired})`)
//       console.log(`webhookResults=`, webhookResults)
//     }
// // console.log(`getWebhooksToProcess() - RETURNING NO WEBHOOKS`.brightRed)
// //     return []

//     const txIdsAndStatuses = [ ]
//     for (const result of webhookResults) {
//       if (typeof(result.txId) === 'undefined') {
//         throw new Error('Missing result.txId')
//       }
//       if (typeof(result.result) === 'undefined') {
//         throw new Error('Missing result.result')
//       }
//       if (typeof(result.comment) === 'undefined') {
//         throw new Error('Missing result.comment')
//       }
//       txIdsAndStatuses.push(result.txId)
//       txIdsAndStatuses.push(result.result)
//       txIdsAndStatuses.push(result.comment)
//     }

//     await RedisLua._checkLoaded()
//     const webhookListKey = `${KEYSPACE_SCHEDULED_WEBHOOK}`
//     try {
//       // if (txIdsAndStatuses.length > 0) {
//       //   console.log(`txIdsAndStatuses=`, txIdsAndStatuses)
//       // }
//       let result
//       try {
//         result = await connection_enqueue.datp_webhooksToProcess(webhookListKey, numRequired, webhookResults.length, txIdsAndStatuses)
//         // if (result.length > 0) {
//         //   console.log(`datp_webhooksToProcess returned`, result)
//         // }
//       } catch (e) {
//         console.log(`FATAL ERROR calling LUA script [datp_webhooksToProcess]`)
//         throw e
//       }
  
//       const batch = [ ]
//       for (const values of result) {
//         switch (values[0]) {
//           case 'warning':
//             // A error occurred. Non-fatal, but report it.
//             console.log(`Warning ${values[1]}: ${values[2]}: ${values[3]}`)
//             console.log(``)
//             break

//           case 'webhook':
//             // console.log(`=========> `, values)
//             const txId = values[1]
//             const state = values[2]
//             const webhook = values[3]
//             const retryCount = values[4]
//             const webhookType = values[5]
//             const completionTime = values[6]
//             const retryTime = values[7]
//             const nextRetryTime = values[8]
//             batch.push({ txId, state, webhook, retryCount, webhookType, completionTime, retryTime, nextRetryTime })
//             break
//         }
//       }
//       // console.log(`getWebhooksToProcess() finished`)
//       return batch
//     } catch (e) {
//       console.log(`e=`, e)
//     }
//   }


  /**
   * Select our pipeline definitions from the database and upload them to REDIS.
   */
  async uploadPipelineDefinitions() {
    // console.log(`uploadPipelineDefinitions()`)
    await RedisLua._checkLoaded()

    const sql = `SELECT
      name,
      version,
      node_name AS nodeGroup,
      steps_json AS definition
    FROM atp_pipeline
    WHERE status='active'`
    const params = [ ]
    const pipelines = await query(sql, await params)

    let sql2 = `SELECT
      ACTIVE.transaction_type AS name,
      ACTIVE.is_transaction_type AS isTransactionType,
      ACTIVE.pipeline_version AS version,
      ACTIVE.node_group AS nodeGroup,
      PIPELINE.steps_json AS definition

    FROM atp_transaction_type ACTIVE, atp_pipeline PIPELINE
    WHERE
      PIPELINE.name = ACTIVE.transaction_type
      AND
      PIPELINE.version = ACTIVE.pipeline_version
      `
    const rows = await query(sql2)
    // console.log(`rows=`.brightMagenta, rows)


    const connection = await RedisLua.regularConnection()
    for (const row of rows) {
      const key = `${KEYSPACE_PIPELINE}${row.name}`
      const nodeGroup = row.nodeGroup ? row.nodeGroup : 'master'
      await connection.hset(key,
        'version', row.version,
        'nodeGroup', nodeGroup,
        'definition', row.definition,
        'isTransactionType', row.isTransactionType
      )
    }
  }

  /**
   * Remove all the pipeline definitions from REDIS.
   * This can be run if ever required, to strip out obsolete pipeline definitions,
   * because uploadPipelineDefinitions only adds definitions, it does not remove any.
   * NOTE: Make sure you reload the definitions!!!
   */
  async flushPipelineDefinitions() {
    console.log(`flushPipelineDefinitions()`)
    await RedisLua._checkLoaded()

    const connection = await RedisLua.regularConnection()
    const pipelines = await connection.keys(`${KEYSPACE_PIPELINE}*`)
    console.log(`pipelines=`, pipelines)
    for (const key of pipelines) {
      console.log(`key=`, key)
      const rv = await connection_admin.hdel(key, 'version', 'nodeGroup', 'definition')
      console.log(`  rv=`, rv)
    }
  }

}

// function externaliseStateField(txStateObject, map, externalisedStateFields) {
//   // externalisedStateFields.push(map.redis)
//   const value = externaliseStateField_extractValue(txStateObject, map.txState)
//   console.log(`=> ${map.txState} = ${value}`)
//   externalisedStateFields.push(value)
// }


/**
 * Extract the externalized fields from the transaction state.
 */
export function externalizeTransactionState(txState) {

  // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
  let txStateObject = txState.asObject()

  let doDelete = true
  if (externalizationMode === EXTERNALIZE_BUT_DONT_DELETE) {
    doDelete = false
  } else if (externalizationMode === EXTERNALIZE_FROM_COPY_AND_DELETE) {
    // We will delete the fields, but from a copy
    txStateObject = deepCopy(txStateObject)
  }

  // If we delete them then the stateJSON is a little bit smaller, but we
  // have a problem because other code might still be using this state object.
  // So, we have three options:

  // Option 1. Don't delete the externalized values.
  // const PUT_VALUES_BACK = false
  // const doDelete = false

  // Option 2. Use a copy of the transaction state.
  // const PUT_VALUES_BACK = false
  // txStateObject = deepCopy(txStateObject)

  // Option 3. Put the values back later
  // const PUT_VALUES_BACK = true
  // const doDelete = true

  const externalisedStateFields = [ ]
  for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
    const value = externaliseStateField_extractValue(txStateObject, map.txState, doDelete)
    // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
    externalisedStateFields.push(value)
  }
  const stateJSON = JSON.stringify(txStateObject)
  return { stateJSON, externalisedStateFields }
}

/**
 * Put the externalized fields back into the transaction state.
 */
export function de_externalizeTransactionState(txState, fieldMapping, fieldValues, firstFieldIndex) {
  // console.log(`de_externalizeTransactionState(firstExternalizedFieldIndex=${firstExternalizedFieldIndex })`)
  // console.log(`fieldValues=`, JSON.stringify(fieldValues, '', 2))
  // if (externalizationMode === EXTERNALIZE_BUT_DONT_DELETE) {
  //   // We didn't take the values out, so we don't need to put them back in.
  //   return
  // }

  const txStateObject = txState.asObject()
  // console.log(`txStateObject BEFORE =`.brightRed, txStateObject)
  for (const [i, map] of fieldMapping.entries()) {
    // const index = firstFieldIndex + i
    // console.log(`${index}: ${map.txState} <== ${i} ${map.redis} = ${fieldValues[firstFieldIndex + i]}    (${typeof(fieldValues[firstExternalizedFieldIndex + i])})`)
    let value = fieldValues[firstFieldIndex + i]
    if (map.type === 'number') {
      value = parseInt(value)
      if (isNaN(value)) {
        value = null
      }
    }
    // console.log(`  ${i} => ${firstFieldIndex+i} => ${value} => ${map.txState}`)
    externaliseStateField_setValue(txStateObject, map.txState, value)
  }
  // console.log(`txStateObject AFTER =`.brightRed, JSON.stringify(txStateObject, '', 2).brightRed)

}


export function transactionStateFromJsonAndExternalizedFields(stateJSON, externalisedStateFields, firstExternalizedFieldIndex, firstReadOnlyFieldIndex=0) {
  // console.log(`transactionStateFromJsonAndExternalizedFields(stateJSON, externalisedStateFields, ${firstExternalizedFieldIndex}, ${firstReadOnlyFieldIndex})`)
  // can be created, because these have been removed from the JSON. Those values
  // will be patched back in below, along with the other externalized fields.
  const txState = new TransactionState(stateJSON)
  // console.log(`\nXCC1: txState.asJSON()=`, txState.asJSON())
  de_externalizeTransactionState(txState, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, externalisedStateFields, firstExternalizedFieldIndex)
  // console.log(`\nXCC2: txState.asJSON()=`, txState.asJSON())
  if (firstReadOnlyFieldIndex) {
    de_externalizeTransactionState(txState, STATE_TO_REDIS_READONLY_MAPPING, externalisedStateFields, firstReadOnlyFieldIndex)
  }
  // console.log(`\nXCC3: txState.asJSON()=`, txState.asJSON())
  // const txStateObject = txState.asObject()
  // // console.log(`txStateObject BEFORE =`.brightRed, txStateObject)
  // for (const [i, map] of STATE_TO_REDIS_EXTERNALIZATION_MAPPING.entries()) {
  //   // console.log(`${map.txState} <== ${i} ${map.redis} = ${fieldValues[firstExternalizedFieldIndex + i]}    (${typeof(fieldValues[firstExternalizedFieldIndex + i])})`)
  //   const value = fieldValues[firstExternalizedFieldIndex + i]
  //   externaliseStateField_setValue(txStateObject, map.txState, value)
  // }
  // console.log(`txStateObject AFTER =`.brightRed, txStateObject)
  return txState
}


export function externaliseStateField_extractValue(object, path, doDelete) {
  const pos = path.indexOf('.')
  if (pos < 0) {

    // Exact name
    const value = object[path]
    if (typeof(value) === 'undefined' || value === null) {
      return null
    }
    if (doDelete) {
      delete object[path]
    }
    return value
  } else {

    // Path is prefix.suffix
    const prefix = path.substring(0, pos)
    const suffix = path.substring(pos + 1)
    const obj = object[prefix]
    if (obj === undefined) {
      return null
    }
    assert(typeof(obj) === 'object')
    // console.log(`${prefix}=`, obj)
    const value = externaliseStateField_extractValue(obj, suffix, doDelete)
    if (doDelete && Object.keys(obj).length === 0) {
      delete object[prefix]
    }
    // console.log(`${suffix}=`, value)
    return value
  }
}


function externaliseStateField_setValue(object, path, value) {
  const pos = path.indexOf('.')
  if (pos < 0) {

    // Exact name
    object[path] = value
  } else {

    // Path is prefix.suffix
    const prefix = path.substring(0, pos)
    const suffix = path.substring(pos + 1)
    let obj = object[prefix]
    if (obj === undefined) {
      // console.log(`Add object ${prefix}`.magenta)
      obj = { }
      object[prefix] = obj
    }
    externaliseStateField_setValue(obj, suffix, value)
  }
}
