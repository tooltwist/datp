import juice from '@tooltwist/juice-client'
import assert from 'assert';
// import { schedulerForThisNode } from '../../..';
import query from '../../../database/query';
import { deepCopy } from '../../../lib/deepCopy';
import pause from '../../../lib/pause';
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_SUCCESS, STEP_TIMEOUT } from '../../Step';
import { STEP_TYPE_PIPELINE } from '../../StepTypeRegister';
import { DEFINITION_STEP_COMPLETE_EVENT, DEFINITION_PROCESS_STEP_START_EVENT, FLOW_DEFINITION, STEP_DEFINITION, validateStandardObject } from '../eventValidation';
import { flow2Msg } from '../flowMsg';
import Scheduler2 from '../Scheduler2';
import TransactionState, { F2ATTR_STEPID } from '../TransactionState';
import Transaction, { TX_STATUS_QUEUED, TX_STATUS_RUNNING } from '../TransactionState';
import { requiresWebhookReply, WEBHOOK_RESULT_ABORTED, WEBHOOK_RESULT_FAILED, WEBHOOK_RESULT_SUCCESS, WEBHOOK_STATUS_ABORTED, WEBHOOK_STATUS_DELIVERED, WEBHOOK_STATUS_PENDING, WEBHOOK_STATUS_PROCESSING, WEBHOOK_STATUS_RETRY, WEBHOOK_STATUS_MAX_RETRIES, WEBHOOK_EVENT_TXSTATUS } from '../webhooks/tryTheWebhook';
const Redis = require('ioredis');

const VERBOSE = 0
const SHOWLOG = 0

export const FLOW_VERBOSE = 0
export const FLOW_PARANOID = 1
let yarpCnt = 0

// Only get the connection once
const CONNECTION_NONE = 0
const CONNECTION_WAIT = 1
const CONNECTION_READY = 2
let connectionStatus = CONNECTION_NONE

// Connections to REDIS
let connection_dequeue = null // mostly blocking, waiting on queue
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
const EXTERNALIZE_BUT_DONT_DELETE = 0
const EXTERNALIZE_FROM_COPY_AND_DELETE = 1 // Why doesn't this work?
const EXTERNALIZE_AND_DELETE_BUT_PUT_BACK_IN = 2 // Why doesn't this work?
const externalizationMode = EXTERNALIZE_BUT_DONT_DELETE



/**
 * Event queues.
 * One of each per nodeGroup.
 * LIST
 */
const KEYSPACE_QUEUE_IN = 'qInL:'
const KEYSPACE_QUEUE_OUT = 'qOutL:'
const KEYSPACE_QUEUE_ADMIN = 'qAdminL:'

/**
 * Transactions currently being processed.
 */
const KEYSPACE_RUNNING = 'processingZ'

/**
 * Transaction state.
 * One per transaction.
 * HASH
 */
const KEYSPACE_STATE = 'stateH:'

const KEYSPACE_EXTERNALID = 'externalId:'
const EXTERNALID_UNIQUENESS_PERIOD = 6 * 60 * 60 // An externalId may nto be reused for this many seconds


/**
 * Transaction state.
 * One per transaction.
 * HASH
 */
const KEYSPACE_STATS_1 = 'metric1H:'
// const STATS_INTERVAL_1 = 15 // 15 seconds
const STATS_INTERVAL_1 = 5
// const STATS_RETENTION_1 = 15 * 60 // 15 minutes
const STATS_RETENTION_1 = 5 * 60
const KEYSPACE_STATS_2 = 'metric2H:'
// const STATS_INTERVAL_2 = 5 * 60 // 5 minutes
const STATS_INTERVAL_2 = 100
const STATS_RETENTION_2 = 2 * 60 * 60 // 2 hours
const KEYSPACE_STATS_3 = 'metric3H:'
// const STATS_INTERVAL_3 = 60 * 60 // 1 hour
const STATS_INTERVAL_3 = 1000
const STATS_RETENTION_3 = 24 * 60 * 60 // 24 hours

/**
 * ZZZZ Do we need this?
 */
const KEYSPACE_SLEEPING = 'sleeping:'

/**
 * IDs of transactions that can be archived to long term storage.
 *  - A list of txIds that should be persisted to long term storage.
 *  - The transaction state remains in the State table.
 *  - Can be sleeping, or complete transactions.
 *  - A dedicated process saves to DB, then removes from State and toArchive.
 *  - Sorted list, with a time-added used as the sort key.
 */
const KEYSPACE_TOARCHIVE = 'toArchiveZ'
const KEYSPACE_TOARCHIVE_LOCK1 = 'toArchiveLock1'
const KEYSPACE_TOARCHIVE_LOCK2 = 'toArchiveLock2'

const PERSISTING_PERMISSION_LOCK1_PERIOD = 60 // A node is given exclusive permission to persist transaction for this many seconds
const PERSISTING_PERMISSION_LOCK2_PERIOD = PERSISTING_PERMISSION_LOCK1_PERIOD + 30
const DELAY_BEFORE_ARCHIVING = 20 // Minutes of delay before archiving and removing from REDIS (gives time for MONDAT to look at the transaction)

/**
 * IDs of completed transactions that need a reply via webhook.
 * The time of the next webhook attempt is used as a sort key (initially zero, with an exponential backoff).
 * - At the time of accessing a record, it gets rescheduled at the next retry time.
 * - If a webhook attempt is successful we delete the txID from the list so there is no retry.
 * - Retry time has an exponential back-off of (30 * 1.8^retryNumber). e.g. 30, 54, 97, 175, 152, 315, etc
 */
const KEYSPACE_SCHEDULED_WEBHOOK = 'webhooksZ'
const KEYSPACE_WEBHOOK_FAIL_COUNTER = 'webhookFail:'
const KEYSPACE_WEBHOOK_FAIL_LIMIT = 10
const KEYSPACE_WEBHOOK_FAIL_PERIOD = 2 * 60
// const WEBHOOK_INITIAL_DELAY = 30 * 1000
// const WEBHOOK_EXPONENTIAL_BACKOFF = 1.8
const WEBHOOK_INITIAL_DELAY = 15 * 1000
const WEBHOOK_EXPONENTIAL_BACKOFF = 1
const MAX_WEBHOOK_RETRIES = 8

/**
 * Pipelne definitions
 */
const KEYSPACE_PIPELINE = 'pipelineH:'

/**
 * pub/sub channel for completed transactions and progress reports.
 */
const CHANNEL_NOTIFY = 'datp-notify'


/*
 *  Mapping between values in the txState and the hash fields when it is stored in REDIS.
 */
const STATE_TO_REDIS_EXTERNALIZATION_MAPPING = [
  { txState: 'owner',                             redis: 'owner' },
  { txState: 'txId',                              redis: 'txId' },
  { txState: 'externalId',                        redis: 'externalId' },
  { txState: 'transactionData.transactionType',   redis: 'transactionType' },
  { txState: 'transactionData.status',            redis: 'status' },
  { txState: 'transactionData.metadata.webhook',  redis: 'webhook' },
  { txState: 'transactionData.startTime',         redis: 'startTime' },
  { txState: 'transactionData.completionTime',    redis: 'completionTime' },
  { txState: 'transactionData.completionNote',    redis: 'completionNote' },
  { txState: 'transactionData.lastUpdated',       redis: 'lastUpdated' },
  { txState: 'transactionData.notifiedTime',      redis: 'notifiedTime' },
  { txState: 'webhook.type',                      redis: 'webhook_type' },
  { txState: 'webhook.retryCount',                redis: 'webhook_retryCount' },
  { txState: 'webhook.status',                    redis: 'webhook_status' },
  { txState: 'webhook.comment',                   redis: 'webhook_comment' },
  { txState: 'webhook.initialAttempt',            redis: 'webhook_initial' },
  { txState: 'webhook.latestAttempt',             redis: 'webhook_latest' },
  { txState: 'webhook.nextAttempt',               redis: 'webhook_next' },
  { txState: 'retry.sleepingSince',               redis: 'retry_sleepingSince' },
  { txState: 'retry.retryCount',                  redis: 'retry_counter' },
  { txState: 'retry.wakeTime',                    redis: 'retry_wakeTime' },
  { txState: 'retry.wakeSwitch',                  redis: 'retry_wakeSwitch' },
  { txState: 'retry.wakeNodeGroup',               redis: 'retry_wakeNodeGroup' },
  { txState: 'retry.wakeStepId',                  redis: 'retry_wakeStepId' },
]
const NUMBER_OF_EXTERNALIZED_FIELDS = STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length



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

    /*
     *  Add an event to a queue.
     */
    // See Learn Lua in 15 minutes (https://tylerneylon.com/a/learn-lua/)
    connection_enqueue.defineCommand("datp_enqueue", {
      numberOfKeys: 5,
      lua: `
        local pipelineKey = KEYS[1]
        local queueKey = KEYS[2]
        local stateKey = KEYS[3]
        local runningKey = KEYS[4]
        local externalIdKey = KEYS[5]

        local queueType = ARGV[1]
        local pipeline = ARGV[2]
        local nodeGroup = ARGV[3] -- Overridden if we have a pipeline.
        local eventJSON = ARGV[4]
        local stateJSON = ARGV[5]
        local txOutputJSON = ARGV[6]
        local owner = ARGV[7]
        local txId = ARGV[8]
        local webhook = ARGV[9]
        local eventType = ARGV[10]
        local isInitialTransaction = ARGV[11]
        local INDEX_OF_EXTERNALIZED_FIELDS = 12
        local log = { }

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

        -- Check this transaction is not already in the queue
        local existingStatus = redis.call('hget', stateKey, 'status')
        if existingStatus == '${TX_STATUS_QUEUED}' then
          local msg = 'Internal Error: datp_enqueue: Transaction is already queued'
          return { 'error', 'E99288', txId, msg }
        end
        log[#log+1] = 'Checked transaction does not already have queued status'

redis.call('set', '@n1', nodeGroup)

        -- If a pipeline is specified, get the nodeGroup from the pipeline definition.
        local pipelineVersion = nil
        if pipeline ~= '' then  
          local details = redis.call('hmget', pipelineKey,
              'version',
              'nodeGroup')
          pipelineVersion = details[1]
          nodeGroup = details[2]
redis.call('set', '@n2', nodeGroup)
          if pipelineVersion == nil then
            local msg = 'Internal Error: datp_enqueue: Unknown pipeline [' .. pipeline .. ']'
            return { 'error', 'E982662', pipeline, msg }
          end
          log[#log+1] = 'Got pipeline definition'

redis.call('set', '@n3', nodeGroup)

          if queueType == 'in' then
            queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
          elseif queueType == 'out' then
            queueKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
          elseif queueType == 'admin' then
            queueKey = '${KEYSPACE_QUEUE_ADMIN}' .. nodeGroup
          end
          log[#log+1] = 'Determined that pipeline uses queue ' .. queueKey
        end

        -- Get the current time
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        log[#log+1] = 'Current time is ' .. now

        -- Save the transaction state
        redis.call('hset', stateKey,
          'txId', txId,
          'status', '${TX_STATUS_QUEUED}',
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
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[15].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+15],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[16].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+16],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[17].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+17],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[18].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+18],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[19].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+19],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[20].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+20],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[21].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+21],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[22].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+22],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[23].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+23]
        )
        if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 24 then
          return { 'error', 'E9626322', 'datp_enqueue', 'Incorrect number of parameters' }
        end
        log[#log+1] = 'Saved the transaction state as JSON and externalized fields'

        -- Get the current queue lengths
        local qInKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
        local qOutKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
        local inputLength = redis.call('llen', qInKey)
        local outputLength = redis.call('llen', qOutKey)


        -- Save the event
        if eventJSON ~= '' then
          redis.call('rpush', queueKey, txId)
          log[#log+1] = 'Added the event to the queue'
        end
        
        -- Save the webhook, if there is one.
        --if webhook ~= '' then
        --  redis.call('hset', stateKey, 'webhook', webhook)
        --end

        -- Remove it from the 'processing' list
        local stateKey = '${KEYSPACE_STATE}' .. txId
        redis.call('zrem', runningKey, txId)
        log[#log+1] = 'Remove the transaction ID from the "processing list"'

        -- Update statistics
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


    /*
     *  Get metrics
     */
    connection_enqueue.defineCommand("datp_metrics", {
      numberOfKeys: 0,
      lua: `
      local prefix = ARGV[1]
      local withExtras = ARGV[2]
      local series = ARGV[3]

      -- Get the current time
      local nowArr = redis.call('TIME')
      local seconds = nowArr[1] + 0
      local millis = math.floor(nowArr[2] / 1000)
      local now = (seconds * 1000) + millis
      --log[#log+1] = 'Current time is ' .. now

      -- default to series 2
      local interval = ${STATS_INTERVAL_2}
      local retention = ${STATS_RETENTION_2}
      local keyspace = '${KEYSPACE_STATS_2}'
      if series == '1' then
        interval = ${STATS_INTERVAL_1}
        retention = ${STATS_RETENTION_1}
        keyspace = '${KEYSPACE_STATS_1}'
      elseif series == '3' then
        interval = ${STATS_INTERVAL_3}
        retention = ${STATS_RETENTION_3}
        keyspace = '${KEYSPACE_STATS_3}'
      end

      local currentTimeslot = math.floor(seconds / interval) * interval
      local numIntervals = math.floor(retention / interval)
      local endPeriod = currentTimeslot
      local startPeriod = endPeriod - (numIntervals * interval)


      local result = { startPeriod, interval, numIntervals, prefix, withExtras, series, '---' }

      -- Look at each timeslot
      for timeslot = startPeriod, endPeriod, interval do
        local key = keyspace .. timeslot

        local values = redis.call('hgetall', key)

        --if 1 == 1 then return result end

        local sample = { }
        for i = 1, #values, 2 do
          local name = values[i]
          local value = values[i+1]

          -- Do we need this field value?
          if prefix == '' then

            -- get the aggregated values only
            if name == 'queued' then
              sample[#sample+1] = name
              sample[#sample+1] = value
            elseif withExtras == '1' then
              if name:find('o:::', 1, true) == 1 then

                -- owner
                -- but exclude owner/pipeline
                if not name:find(':::p:::', 1, true) then
                  sample[#sample+1] = name
                  sample[#sample+1] = value
                end
              elseif name:find('p:::', 1, true) == 1 then

                -- pipeline
                sample[#sample+1] = name
                sample[#sample+1] = value
              elseif name:find('g:::', 1, true) == 1 then

                -- group
                sample[#sample+1] = name
                sample[#sample+1] = value
              elseif name:find('tx:::', 1, true) == 1 then

                -- transaction
                sample[#sample+1] = name
                sample[#sample+1] = value
              elseif name:find('iLen:::', 1, true) == 1 then

                -- input queue length
                sample[#sample+1] = name
                sample[#sample+1] = value
              elseif name:find('oLen:::', 1, true) == 1 then

                -- output queue length
                sample[#sample+1] = name
                sample[#sample+1] = value
              end
            end
          else

            -- Get requested fields only
            --sample[#sample+1] = name
            --sample[#sample+1] = 'WITH prefix'
            if name:find(prefix, 1, true) == 1 then
              sample[#sample+1] = name
              sample[#sample+1] = value
            end
          end
        end

        -- If we found some values, add this time period to the result
        if #sample > 0 then
          sample[#sample+1] = '-'
          sample[#sample+1] = timeslot
          result[#result+1] = sample
        end
        --local value = redis.call('hget', key, 'in')
        --if value then
        --  result[#result+1] = timeslot
        --  --result[#result+1] = key
        --  result[#result+1] = value
        --  result[#result+1] = '-'
        --end
      end
      return result
      `
    })//- datp_metrics


    /*
     *  Get a transaction state.
     */
    connection_enqueue.defineCommand("datp_findTransactions", {
      numberOfKeys: 0,
      lua: `
      local filter = KEYS[1]

      if 1 == 2 then
        return { 'error', 'I dun lyke Mondese yarp ZZZZZ' }
      end

      -- This is horrible.
      -- We need to look at EVERY key to find the transaction status keys
      local keys = redis.call('keys', '${KEYSPACE_STATE}*')



      local result = { }
      for i = 1, #keys do
        local state = redis.call('hmget', keys[i], 'txId', 'externalId', 'transactionType', 'status', 'retry_wakeSwitch', 'startTime', 'lastUpdated')
        local status = state[4]
        result[#result+1] = state
      end
      if 1 == 1 then
        return result
      end

      -- Perhaps mark that the reply has (or is about to be) sent to the client
      if markAsReplied == 'true' then
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        redis.call('hset', stateKey, 'notifiedTime', now)
        log[#log+1] = 'Setting transaction notifiedTime'
      end


      local fields = redis.call('hmget', stateKey,
        'txId',
        'status',
        'event',
        'state',
        --'eventType', 'pipeline', 'ts', 'queue', extras)
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[0].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[1].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[2].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[3].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[4].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[5].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[6].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[7].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[8].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[9].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[10].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[11].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[12].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[13].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[14].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[15].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[16].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[17].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[18].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[19].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[20].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[21].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[22].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[23].redis}'
      )
      if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 24 then
        return { 'error', 'Internal error #992822: Incorrect number of parameters' }
      end
      log[#log+1] = 'Selected JSON and externalized fields for the transaction state'

      -- Perhaps cancel the webhook
      -- This only happens if we specify that polling success
      local txId = fields[1]
      if txId then
        if cancelWebhook == 'true' then
          redis.call('zrem', webhookKey, txId)
          --redis.call('set', '@cancelled-webhook-' .. txId, 'yep')
          log[#log+1] = 'Cancelled the webhook'
          end
      end

      fields[#fields+1] = log
      return fields
    `})//- datp_findTransactions


    /*
     *  Get a transaction state.
     */
    connection_enqueue.defineCommand("datp_getState", {
      numberOfKeys: 2,
      lua: `
      local stateKey = KEYS[1]
      local webhookKey = KEYS[2]
      local markAsReplied = ARGV[1]
      local cancelWebhook = ARGV[2]
      local log = { }


      -- Perhaps mark that the reply has (or is about to be) sent to the client
      if markAsReplied == 'true' then
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        redis.call('hset', stateKey, 'notifiedTime', now)
        log[#log+1] = 'Setting transaction notifiedTime'
      end


      local fields = redis.call('hmget', stateKey,
        'txId',
        'status',
        'event',
        'state',
        --'eventType', 'pipeline', 'ts', 'queue', extras)
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[0].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[1].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[2].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[3].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[4].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[5].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[6].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[7].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[8].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[9].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[10].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[11].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[12].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[13].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[14].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[15].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[16].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[17].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[18].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[19].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[20].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[21].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[22].redis}',
        '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[23].redis}'
      )
      if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 24 then
        return { 'error', 'Internal error #992822: Incorrect number of parameters' }
      end
      log[#log+1] = 'Selected JSON and externalized fields for the transaction state'

      -- Perhaps cancel the webhook
      -- This only happens if we specify that polling success
      local txId = fields[1]
      if txId then
        if cancelWebhook == 'true' then
          redis.call('zrem', webhookKey, txId)
          --redis.call('set', '@cancelled-webhook-' .. txId, 'yep')
          log[#log+1] = 'Cancelled the webhook'
          end
      end

      fields[#fields+1] = log
      return fields
    `})//- datp_getState

    /*
     *  Get events from queues
     */
    connection_enqueue.defineCommand("datp_dequeue", {
      numberOfKeys: 5,
      lua: `
        local qInKey = KEYS[1]
        local qOutKey = KEYS[2]
        local qAdminKey = KEYS[3]
        local runningKey = KEYS[4]
        local pipelinesKey = KEYS[5]
        local nodeGroup = ARGV[1]
        local required = tonumber(ARGV[2])
        local log = { }

        -- Get the current time
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis

        -- We'll create a list to be returned
        local list = { }


        -- Look in the admin queue first
        -- These need to be treated differently, as they might not involve a transaction
        if required > 0 then
          local events = redis.call('LPOP', qAdminKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              list[#list + 1] = { 'admin', events[i] }
              required = required - 1
            end
          end
        end

        -- Look in the output queue next
        local txIds = { }
        if required > 0 then
          local events = redis.call('LPOP', qOutKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              local txId = events[i]
              txIds[#txIds+1] = txId
              required = required - 1
            end
          end
        end
        -- Look in the input queue next
        if required > 0 then
          local events = redis.call('LPOP', qInKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              local txId = events[i]
              txIds[#txIds+1] = txId
              required = required - 1
            end
          end
        end

        -- For each transaction ID (from the output and input queues) get
        -- the JSON and externalized fields.
        log[#log+1] = 'For each transaction:'
        log[#log+1] = ' - select JSON and externalized fields from each transaction state'
        log[#log+1] = ' - perhaps get the pipeline definition'
        log[#log+1] = ' - add it to our event list'
        log[#log+1] = ' - change the status to running'
        log[#log+1] = ' - add it to the list of events being processed'

        for i = 1, #txIds do
          local txId = txIds[i]
          local stateKey = '${KEYSPACE_STATE}' .. txId
          local txStateValues = redis.call('hmget', stateKey,
              'owner',
              'status',
              'event',
              'state',
              'pipeline',
              --'eventType'
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[0].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[1].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[2].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[3].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[4].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[5].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[6].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[7].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[8].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[9].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[10].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[11].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[12].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[13].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[14].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[15].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[16].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[17].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[18].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[19].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[20].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[21].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[22].redis}',
              '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[23].redis}'
          )
          if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 24 then
            return { 'error', 'Internal error #912412: Incorrect number of parameters' }
          end

          local owner = txStateValues[1]
          local status = txStateValues[2]
          local pipeline = txStateValues[5]
  
          -- Decide what to do
          if not status then

            -- Event is in queue, but we don't have a status. ERROR!
            -- This should not happen. Someone has changed the status while it is in the queue.
            local msg = 'Internal Error: Transaction in event queue has no state saved [' .. txId .. ']'
            list[#list + 1] = { 'error', 'E37726', txId, msg }
          elseif status == '${TX_STATUS_QUEUED}' then

            -- We have an event to process
            -- Perhaps get the pipeline definition
            -- Add it to our event list
            -- Change the status to 'running'
            -- Add it to the list of events being processed
            local pipelineDefinitionJSON = '-'
            if pipeline then
              local pipelineKey = '${KEYSPACE_PIPELINE}' .. pipeline
              local details = redis.call('hmget', pipelineKey, 'definition', 'nodeGroup')
              pipelineDefinitionJSON = details[1]
            end
            txStateValues[#txStateValues+1] = pipelineDefinitionJSON

            -- Add this transaction's details to our list
            list[#list + 1] = { 'event', txStateValues }

            -- Set the status to running
            redis.call('hset', stateKey, 'status', '${TX_STATUS_RUNNING}', 'queue', '')

            -- Add this transaction to the list of currently running transactions
            redis.call('zadd', runningKey, now, txId)

            -- Get the current queue lengths
            local inputLength = redis.call('llen', qInKey)
            local outputLength = redis.call('llen', qOutKey)
    
            -- Update statistics
            -- STATS TOP
            if 1 == 1 then
            local f_start = 'start'
            local f_nodeGroup = 'g:::' .. nodeGroup .. ':::start'
            local f_ownerStart = 'o:::' .. owner .. ':::start'
            local f_ownerPipeline = 'o:::' .. owner .. ':::p:::' .. pipeline .. ':::start'
            local f_pipelineStart = 'p:::' .. pipeline .. ':::start'
            local f_qInLength = 'iLen:::' .. nodeGroup
            local f_qOutLength = 'oLen:::' .. nodeGroup
    
            -- Short interval
            local timeslot1 = math.floor(seconds / ${STATS_INTERVAL_1}) * ${STATS_INTERVAL_1}
            local expiry1 = timeslot1 + ${STATS_RETENTION_1}
            local statsKey1 = '${KEYSPACE_STATS_1}' .. timeslot1

            redis.call('hincrby', statsKey1, f_start, 1)
            redis.call('hincrby', statsKey1, f_nodeGroup, 1)
            redis.call('hincrby', statsKey1, f_ownerStart, 1)
            --if pipeline ~= '' then
              redis.call('hincrby', statsKey1, f_ownerPipeline, 1)
              redis.call('hincrby', statsKey1, f_pipelineStart, 1)
            --end
            redis.call('hset', statsKey1, f_qInLength, inputLength)
            redis.call('hset', statsKey1, f_qOutLength, outputLength)
            redis.call('expireat', statsKey1, expiry1)
    
            -- Medium interval
            local timeslot2 = math.floor(seconds / ${STATS_INTERVAL_2}) * ${STATS_INTERVAL_2}
            local expiry2 = timeslot2 + ${STATS_RETENTION_2}
            local statsKey2 = '${KEYSPACE_STATS_2}' .. timeslot2
            redis.call('hincrby', statsKey2, f_start, 1)
            redis.call('hincrby', statsKey2, f_nodeGroup, 1)
            redis.call('hincrby', statsKey2, f_ownerStart, 1)
            --if pipeline ~= '' then
            redis.call('hincrby', statsKey2, f_ownerPipeline, 1)
            redis.call('hincrby', statsKey2, f_pipelineStart, 1)
            --end
            redis.call('hset', statsKey2, f_qInLength, inputLength)
            redis.call('hset', statsKey2, f_qOutLength, outputLength)
            redis.call('expireat', statsKey2, expiry2)
    
            -- Long interval
            local timeslot3 = math.floor(seconds / ${STATS_INTERVAL_3}) * ${STATS_INTERVAL_3}
            local expiry3 = timeslot3 + ${STATS_RETENTION_3}
            local statsKey3 = '${KEYSPACE_STATS_3}' .. timeslot3
            redis.call('hincrby', statsKey3, f_start, 1)
            redis.call('hincrby', statsKey3, f_nodeGroup, 1)
            redis.call('hincrby', statsKey3, f_ownerStart, 1)
            --if pipeline ~= '' then
            redis.call('hincrby', statsKey3, f_ownerPipeline, 1)
            redis.call('hincrby', statsKey3, f_pipelineStart, 1)
            --end
            redis.call('hset', statsKey3, f_qInLength, inputLength)
            redis.call('hset', statsKey3, f_qOutLength, outputLength)
            redis.call('expireat', statsKey3, expiry3)
            log[#log+1] = ' - updated the metrics collection'
            -- STATS BOTTOM
            end


          elseif status == 'cancelled' then

        
            -- Ignore cancelled transactions
          else

            -- This should not happen. Someone has changed the status while it is in the queue. ERROR!
            local msg = 'Transaction had status changed to [' .. status .. '] while in output queue [' .. qOutKey .. ']'
            list[#list + 1] = { 'error', 'E8827736b', txId, msg }
          end
        end

        -- Add the log to the result, and return
        list[#list+1] = { 'log', log }
        return list
      `
    })//- datp_dequeue

    /*
     *  Complete a transaction.
     */
    connection_enqueue.defineCommand("datp_completeTransaction", {
      numberOfKeys: 4,
      lua: `
        local stateKey = KEYS[1]
        local webhookKey = KEYS[2]
        local runningKey = KEYS[3]
        local persistKey = KEYS[4]
        local txId = ARGV[1]
        local status = ARGV[2]
        local state = ARGV[3]
        local INDEX_OF_EXTERNALIZED_FIELDS = 4
        local log = { }

        -- Check the existing status of the transaction and get the webhook
        -- The webhook was saved at the time we created the state
        local existingValues = redis.call('hmget', stateKey, 'status', 'webhook')
        local existingStatus = existingValues[1]
        local webhook = existingValues[2]
        if not existingStatus then
          local msg = 'Internal Error: Transaction to be completed has no state saved [' .. txId .. ']'
          return { 'error', 'E662622', txId, msg }
        end
        log[#log+1] = 'Verified that transaction state exists in REDIS'

        -- Check the completion status is valid
        if status ~= '${STEP_ABORTED}' and status ~= '${STEP_FAILED}' and status ~= '${STEP_INTERNAL_ERROR}' and status ~= '${STEP_SUCCESS}' and status ~= '${STEP_TIMEOUT}' then
          local msg = 'Internal Error: Cannot complete transaction with status "' .. status .. '" [' .. txId .. ']'
          return { 'error', 'E686221', txId, msg }
        end
        log[#log+1] = 'New status [' .. status .. '] is valid'

        -- Check the existing status allows completion
        if existingStatus ~= 'running' then
          local msg = 'Internal Error: Cannot complete transaction with current status "' .. existingStatus .. '" [' .. txId .. ']'
          return { 'error', 'E662752', txId, msg }
        end
        log[#log+1] = 'Existing status [' .. existingStatus .. '] allows completion to proceed'

        -- What is the current time?
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        log[#log+1] = 'Current time is ' .. now

        -- Update the status, state, and remove the latest event
        redis.call('hset', stateKey,
          'status', status,
          'state', state,
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
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[15].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+15],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[16].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+16],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[17].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+17],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[18].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+18],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[19].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+19],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[20].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+20],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[21].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+21],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[22].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+22],
          '${STATE_TO_REDIS_EXTERNALIZATION_MAPPING[23].redis}', ARGV[INDEX_OF_EXTERNALIZED_FIELDS+23]
        )
        if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 24 then
          return { 'error', 'Internal error #9626322: Incorrect number of parameters' }
        end
        log[#log+1] = 'Updated transaction state and externalized fields'

        -- Remove any event (there should be none)
        redis.call('hdel', stateKey, 'event', 'eventType')
        log[#log+1] = 'Removed event from transaction state'

        -- If there is a webhook, schedule the webhook call
        local yarpItWebhook = 'NO WEBHOOK'
        if webhook == '' then
          log[#log+1] = 'Does not have a webhook'
        else
          -- Add to the webhook queue
          -- Set the webhook details in the transaction state hash
          yarpItWebhook = 'SET WEBHOOK'
          redis.call('zadd', webhookKey, now, txId)
          redis.call('hset', stateKey,
              'webhook_status', '${WEBHOOK_STATUS_PENDING}',
              'webhook_time', now,
              'webhook_retryCount', 1,
              'webhook_comment', '',
              'webhook_type', '${WEBHOOK_EVENT_TXSTATUS}')
          log[#log+1] = 'Added [' .. webhook .. '] to the webhook queue'
        end

        -- Schedule the state to be archived
        -- Not immediate, because API client may want to read the state
        local later = now + (${DELAY_BEFORE_ARCHIVING} * 60 * 1000)
        redis.call('zadd', persistKey, later, txId)
        log[#log+1] = 'Scheduled for archiving in ${DELAY_BEFORE_ARCHIVING} minutes'

        -- Publish notification of the transaction completion
        local message = 'completed:' .. txId .. ':' .. status
        redis.call('publish', '${CHANNEL_NOTIFY}', message )
        log[#log+1] = 'Published the transaction completion to channel ' .. '${CHANNEL_NOTIFY}'
    

        
        -- All good
        return { 'ok', txId, status, log }
    `}) //- datp_completeTransaction


    /*
     *  Get IDs of transactions that need to be persisted.
     *  - Receives as input a list of transactions that have been successfully persisted
     *    after a previous call to this function, so we can emove them from the list.
     *  - We perform a simplistic lock using an expiry time to ensure that only one node
     *    is running this at any time. The first node to call this function will get a
     *    list of transactions to persist, and for a period of time any other node will
     *    be returned nothing.
     *  - We want a grace period before another node gets permission to do the persisting.
     *    To manage this, we have a second lock. When the first lock is expired but the
     *    second lock still exists, no other node will be given permission. This gives
     *    the first node time to finish any persisting it is doing and re-call this
     *    function to notify they have been done, without some other node being given the
     *    same transaction IDs to persist.
     */
    connection_enqueue.defineCommand("datp_transactionsToArchive", {
      numberOfKeys: 3,
      lua: `
        local toArchiveKey = KEYS[1]
        local toArchiveLockKey1 = KEYS[2]
        local toArchiveLockKey2 = KEYS[3]
        local nodeId = ARGV[1]
        local numRequired = tonumber(ARGV[2])
        local numPersisted = tonumber(ARGV[3])
        
        -- Remove sucessfully persisted transactions from the list to be archived
        for i = 0, numPersisted-1 do
          local txId = ARGV[4 + i]
          -- Remove the state
          local stateKey = '${KEYSPACE_STATE}' .. txId
          redis.call('del', stateKey)
          -- remove the value from the archiving queue
          redis.call('zrem', toArchiveKey, txId)
        end

        -- Perhaps we aren't after any more to archive...
        if numRequired < 1 then
          return { }
        end

        -- Check this node is the one persisting transactions.
        local permittedNode = redis.call('get', toArchiveLockKey1)
        if not permittedNode then

          -- No node currently is given permission to do persisting
          -- Check that lock2 has expired
          if redis.call('get', toArchiveLockKey2) then
            return { }
          end

          -- Okay, give this node permission
          redis.call('set', toArchiveLockKey1, nodeId)
          redis.call('expire', toArchiveLockKey1, ${PERSISTING_PERMISSION_LOCK1_PERIOD})
          redis.call('set', toArchiveLockKey2, nodeId)
          redis.call('expire', toArchiveLockKey2, ${PERSISTING_PERMISSION_LOCK2_PERIOD})
        elseif permittedNode ~= nodeId then

          -- Some other node is doing persisting
          return { }
        else

          -- This node already has permission to do persisting
        end

        -- Get a list of transactions needing to be persisted
        local list = { }
        local txIds = redis.call('zpopmin', toArchiveKey, numRequired)
        if txIds then
          -- Add transaction states to our output
          -- txIds = [ txId1, sortOrder1, txId2, sortOrder2, ...]
          local num = #txIds / 2
          for i = 0, num-1 do
            local txId = txIds[1 + (i*2)]
            local stateKey = '${KEYSPACE_STATE}' .. txId
            local txState = redis.call('hmget', stateKey, 'status', 'event', 'state', 'pipeline', 'eventType')
            local status = txState[1]
            local event = txState[2]
            local state = txState[3]
            local pipeline = txState[4]
            local eventType = txState[5]

            if not status then
              -- Event is in list to be persisted, but we don't have a status/state!
              local msg = 'Internal Error: Transaction in archiving queue has no state saved [' .. txId .. ']'
              list[#list + 1] = { 'error', 'E37726', txId, msg }
            else
              list[#list + 1] = { 'transaction', txId, state }
            end
          end
        end
        return list
    `}) //- datp_transactionsToArchive


    /*
     *  Get details of transactions that need to a webhook reply.
     *  - Receives as input the results from procesing the previous batch.
     *  - The sort order of the webhook Z list is the time to next try the webhook.
     *  - At the time we pull elements off the list, we schedule them for retry.
     *  - If we are told in the subsequent call that the webhook succeeded, we
     *    remove it from the list.
     */
    connection_enqueue.defineCommand("datp_webhooksToProcess", {
      numberOfKeys: 1,
      lua: `
        local webhookListKey = KEYS[1]
        local numRequired = tonumber(ARGV[1])
        local numResults = tonumber(ARGV[2])
        -- txId, result, comment
        local FIRST_RESULT_INDEX = 3
        local NUM_PARAMS_PER_RESULT = 3

        --if numResults > 0 then
        --return { 'yarpik 999', numResults, FIRST_RESULT_INDEX, NUM_PARAMS_PER_RESULT }
        --end

        -- Get the current time
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis

        -- Remove sucessfully persisted transactions from the list to be archived
        for index = FIRST_RESULT_INDEX, #ARGV-1, NUM_PARAMS_PER_RESULT do
          -- each result has 3 values: txId, result, message
          local txId = ARGV[index]
          local result = ARGV[index + 1]
          local comment = ARGV[index + 2]
          --if 1 == 1 then
          --  return { 'yarpik', txId, result, comment }
          --end

          local stateKey = '${KEYSPACE_STATE}' .. txId
          if result == '${WEBHOOK_RESULT_SUCCESS}' then

            -- Complete, remove the value from the webhook list.
            redis.call('zrem', webhookListKey, txId)
            redis.call('hset', stateKey,
                'webhook_status', '${WEBHOOK_STATUS_DELIVERED}',
                'webhook_time', now,
                'webhook_comment', comment)
          elseif result == '${WEBHOOK_RESULT_FAILED}' then

            -- Not successful. Leave it in the list to retry, but increment the retryCounter
            local newCount = redis.call('hincrby', stateKey, 'webhook_retryCount', 1)
            newCount = tonumber(newCount)
            if newCount >= ${MAX_WEBHOOK_RETRIES} then

              -- Too many attempts, we have to abort
              -- Remove it from the webhook list, and update the tx state
              redis.call('zrem', webhookListKey, txId)
              redis.call('hset', stateKey,
                  'webhook_status', '${WEBHOOK_STATUS_MAX_RETRIES}',
                  'webhook_time', now,
                  'webhook_comment', comment)
            else

              -- Leave it in the webhook list to retry
              redis.call('hset', stateKey,
                'webhook_status', '${WEBHOOK_STATUS_RETRY}',
                'webhook_time', now,
                'webhook_comment', comment)
            end
          end

          -- If we decided to abort the webhook...
          if result == '${WEBHOOK_RESULT_ABORTED}' then

            -- Remove it from the webhook list, and update the tx state
            redis.call('zrem', webhookListKey, txId)
            redis.call('hset', stateKey,
                'webhook_status', '${WEBHOOK_STATUS_ABORTED}',
                'webhook_time', now,
                'webhook_comment', comment)
          end
      end

        -- Perhaps we aren't after any more webhooks...
        if numRequired < 1 then
          return { }
        end


        -- Get a list of transactions needing a webhook to be called
        local list = { }
        local txIds = redis.call('zrangebyscore', webhookListKey, 0, now, 'WITHSCORES', 'LIMIT', 0, numRequired)
        if txIds then
          -- Add transaction states to our output
          -- txIds = [ txId1, sortOrder1, txId2, sortOrder2, ...]
          local num = #txIds / 2
          for i = 0, num-1 do
            local txId = txIds[1 + (i*2)]
            local retryTime = txIds[2 + (i*2)]
            local stateKey = '${KEYSPACE_STATE}' .. txId
            --redis.call('hincrby', stateKey, 'webhook_retryCount', 1)
            local txState = redis.call('hmget', stateKey,
              'state',
              'webhook',
              'webhook_retryCount',
              'webhook_type',
              'completionTime')
            local state = txState[1]
            local webhook = txState[2]
            local webhook_retryCount = tonumber(txState[3])
            local webhook_type = txState[4]
            local completionTime = txState[5]

            if not state then
              -- Event is in list for a webhook, but we don't have a status/state!
              local msg = 'Internal Error: Transaction in webhook queue has no state saved [' .. txId .. ']'
              list[#list + 1] = { 'error', 'E8266112', txId, msg }
              redis.call('zrem', webhookListKey, txId)
            else

              -- Remove, then re-add with a new retryTime
              local delay = ${WEBHOOK_INITIAL_DELAY}
              for i = 0, webhook_retryCount-1 do
                delay = delay * ${WEBHOOK_EXPONENTIAL_BACKOFF}
              end
              delay = math.floor(delay)
              local nextRetryTime = now + delay
              redis.call('zrem', webhookListKey, txId)
              redis.call('zadd', webhookListKey, nextRetryTime, txId)
              redis.call('hset', stateKey,
                  'webhook_status', '${WEBHOOK_STATUS_PROCESSING}',
                  'webhook_time', nextRetryTime,
                  'webhook_delay', delay,
                  'webhook_comment', '')

              list[#list + 1] = { 'webhook', txId, state, webhook, webhook_retryCount, webhook_type, completionTime, retryTime, nextRetryTime, 2^5 }
            end
          end
        end
        return list
    `}) //- LUA: datp_webhooksToProcess


    // Ready for action!
    if (VERBOSE>1) console.log(`{REDIS connection ready}`.gray)
    connectionStatus = CONNECTION_READY

  }//- _checkLoaded


  /**
   * Add an event to a queue.
   * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
   * 
   * If pipeline is specified, the pipeline definition is used to determine the nodegroup,
   * and the pipeline definition will be appended to the reply when the event is dequeued.
   * 
   * @param {string} queueType in | out | admin
   * @param {string} pipeline If provided, we'll get the nodeGroup from the pipelne
   * @param {string} nodeGroup
   * @param {object} event
   */
  async luaEnqueue_pipelineStart(txState, event, pipeline, checkExternalIdIsUnique, nodeGroup=null) {
    if (FLOW_VERBOSE) flow2Msg(txState, `----- luaEnqueue_pipelineStart(txState, event, pipeline=${pipeline}, checkExternalIdIsUnique=${checkExternalIdIsUnique}, nodeGroup=${nodeGroup})`.gray)
    // console.log(`txState=${txState.pretty()}`.magenta)

    const queueType = 'in'
    let externalIdKey = ''
    if (checkExternalIdIsUnique) {
      // console.log(`NEED TO CHECK EXTERNALID IS UNIQUE`.magenta)
      const owner = txState.getOwner()
      const externalId = txState.getExternalId()
      externalIdKey = `${KEYSPACE_EXTERNALID}${owner}:::${externalId}` // If set, we check it's unique and save the mapping
      // console.log(`externalIdKey=`, externalIdKey)
    }

    if (FLOW_PARANOID) {
      validateStandardObject('luaEnqueue_pipelineStart() event', event, DEFINITION_PROCESS_STEP_START_EVENT)
      assert(txState instanceof Transaction)
      txState.vog_validateSteps()
    }

    // Must specify either pipeline or a nodeFroup
    assert(pipeline || nodeGroup)
    assert( !(pipeline && nodeGroup))

    await RedisLua._checkLoaded()

    const eventType = event.eventType

    const txId = txState.getTxId()
    const owner = txState.getOwner()
    txState.vog_setStatusToQueued()

    const metadata = txState.vog_getMetadata()
    const webhook = (metadata.webhook) ? metadata.webhook : ''

    // Save the txState and the event, and perhaps the webhook
    let queueKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`

// console.log(`event=`.magenta, event)
    const tmpTxState = event.txState
    delete event.txState //ZZZ Should not be set
    const eventJSON = JSON.stringify(event)
    event.txState = tmpTxState
// console.log(`eventJSON=`.magenta, eventJSON)

    // Is this the initial transaction?
    const txStateObject = txState.asObject()
    const isInitialTransaction = (txStateObject.f2.length <= 3)
    // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


    const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const runningKey = `${KEYSPACE_RUNNING}`

    const transactionOutputJSON = txState.getTransactionOutputAsJSON()

    const { stateJSON, externalisedStateFields } = externalizeTransactionState(txState)
    // console.log(`stateJSON=`, stateJSON)
    // console.log(`externalisedStateFields=`, externalisedStateFields)

    // // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
    // const externalisedStateFields = [ ]
    // let txStateObject = txState.asObject()

    // // Extract the externalized fields from the transaction state.
    // // If we delete them then the stateJSON is a little bit smaller, but we
    // // have a problem because other code might still be using this state object.
    // // So, we have three options:

    // // Option 1. Don't delete the externalized values.
    // // const PUT_VALUES_BACK = false
    // // const doDelete = false

    // // Option 2. Use a copy of the transaction state.
    // const PUT_VALUES_BACK = false
    // txStateObject = deepCopy(txStateObject)

    // // Option 3. Put the values back later
    // // const PUT_VALUES_BACK = true
    // // const doDelete = true

    // for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
    //   const value = externaliseStateField_extractValue(txStateObject, map.txState)
    //   // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
    //   externalisedStateFields.push(value)
    // }


    // console.log(`externalisedStateFields=`, externalisedStateFields)
    let result
    try {
      result = await connection_enqueue.datp_enqueue(
          // Keys
          pipelineKey, queueKey, stateKey, runningKey, externalIdKey,
          // Parameters
          'in', pipeline, nodeGroup,
          eventJSON, stateJSON, transactionOutputJSON,
          owner, txId, webhook,
          eventType,
          isInitialTransaction ? 'true' : 'false',
          externalisedStateFields)
      // console.log(`datp_enqueue returned`, result)

      switch (result[0]) {
        case 'duplicate':
          throw new Error(`CANNOT CREATE A DUPLICATE TRANSACTION`)
  
        case 'error':
          const msg = `${result[1]}: ${result[2]}: ${result[3]}`
          console.log(msg)
          throw new Error(msg)
  
        case 'ok':
          const log = result[6]
          if (SHOWLOG) console.log(`datp_enqueue =>`, log)
          const reply = {
            queue: result[1],
            nodeGroup: result[2],
            pipelineName: result[3],
            pipelineVersion: result[4],
            webhook: result[5] ? result[5] : null
          }

          // Put the extracted values back in (if required)
          de_externalizeTransactionState(txState, externalisedStateFields, 0)
          // console.log(`State afterwards=${txState.stringify()}`)
          return reply
      }
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_enqueue]`)
      throw e
    }
  }//- luaEnqueue_pipelineStart

  /**
   * Add an event to a queue.
   * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
   * 
   * If pipeline is specified, the pipeline definition is used to determine the nodegroup,
   * and the pipeline definition will be appended to the reply when the event is dequeued.
   * 
   * @param {string} queueType in | out | admin
   * @param {string} pipeline If provided, we'll get the nodeGroup from the pipelne
   * @param {string} nodeGroup
   * @param {object} event
   */
   async luaEnqueue_pipelineEnd(txState, event, pipeline, checkExternalIdIsUnique, nodeGroup=null) {
    // if (FLOW_VERBOSE)
    console.log(`----- luaEnqueue_pipelineEnd(txState, event, pipeline=${pipeline}, checkExternalIdIsUnique=${checkExternalIdIsUnique}, nodeGroup=${nodeGroup})`.gray)
    // console.log(`txState=${txState.pretty()}`.magenta)

    const queueType = 'out'

    let externalIdKey = ''
    if (checkExternalIdIsUnique) {
      // console.log(`NEED TO CHECK EXTERNALID IS UNIQUE`.magenta)
      const owner = txState.getOwner()
      const externalId = txState.getExternalId()
      externalIdKey = `${KEYSPACE_EXTERNALID}${owner}:::${externalId}` // If set, we check it's unique and save the mapping
      // console.log(`externalIdKey=`, externalIdKey)
    }

    if (FLOW_PARANOID) {
        validateStandardObject('luaEnqueue_pipelineEnd() event', event, DEFINITION_STEP_COMPLETE_EVENT)
      assert(txState instanceof Transaction)
      txState.vog_validateSteps()
    }

    // Must specify either pipeline or a nodeFroup
    assert(pipeline || nodeGroup)
    assert( !(pipeline && nodeGroup))

    await RedisLua._checkLoaded()

    const eventType = event.eventType

    const txId = txState.getTxId()
    const owner = txState.getOwner()
    txState.vog_setStatusToQueued()

    const metadata = txState.vog_getMetadata()
    const webhook = (metadata.webhook) ? metadata.webhook : ''

    // Save the txState and the event, and perhaps the webhook
    let queueKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`

// console.log(`event=`.magenta, event)
    const tmpTxState = event.txState
    delete event.txState //ZZZ Should not be set
    const eventJSON = JSON.stringify(event)
    event.txState = tmpTxState
// console.log(`eventJSON=`.magenta, eventJSON)

    // Is this the initial transaction?
    const txStateObject = txState.asObject()
    const isInitialTransaction = (txStateObject.f2.length <= 3)
    // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


    const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const runningKey = `${KEYSPACE_RUNNING}`

    const transactionOutputJSON = txState.getTransactionOutputAsJSON()

    const { stateJSON, externalisedStateFields } = externalizeTransactionState(txState)
    // console.log(`stateJSON=`, stateJSON)
    // console.log(`externalisedStateFields=`, externalisedStateFields)

    // // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
    // const externalisedStateFields = [ ]
    // let txStateObject = txState.asObject()

    // // Extract the externalized fields from the transaction state.
    // // If we delete them then the stateJSON is a little bit smaller, but we
    // // have a problem because other code might still be using this state object.
    // // So, we have three options:

    // // Option 1. Don't delete the externalized values.
    // // const PUT_VALUES_BACK = false
    // // const doDelete = false

    // // Option 2. Use a copy of the transaction state.
    // const PUT_VALUES_BACK = false
    // txStateObject = deepCopy(txStateObject)

    // // Option 3. Put the values back later
    // // const PUT_VALUES_BACK = true
    // // const doDelete = true

    // for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
    //   const value = externaliseStateField_extractValue(txStateObject, map.txState)
    //   // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
    //   externalisedStateFields.push(value)
    // }


    // console.log(`externalisedStateFields=`, externalisedStateFields)
    let result
    try {
      result = await connection_enqueue.datp_enqueue(
          // Keys
          pipelineKey, queueKey, stateKey, runningKey, externalIdKey,
          // Parameters
          'out', pipeline, nodeGroup,
          eventJSON, stateJSON, transactionOutputJSON,
          owner, txId, webhook,
          eventType,
          isInitialTransaction ? 'true' : 'false',
          externalisedStateFields)
      // console.log(`datp_enqueue returned`, result)

      switch (result[0]) {
        case 'duplicate':
          throw new Error(`CANNOT CREATE A DUPLICATE TRANSACTION`)
  
        case 'error':
          const msg = `${result[1]}: ${result[2]}: ${result[3]}`
          console.log(msg)
          throw new Error(msg)
  
        case 'ok':
          const log = result[6]
          if (SHOWLOG) console.log(`datp_enqueue =>`, log)
          const reply = {
            queue: result[1],
            nodeGroup: result[2],
            pipelineName: result[3],
            pipelineVersion: result[4],
            webhook: result[5] ? result[5] : null
          }

          // Put the extracted values back in (if required)
          de_externalizeTransactionState(txState, externalisedStateFields, 0)
          // console.log(`State afterwards=${txState.stringify()}`)
          return reply
      }
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_enqueue]`)
      throw e
    }
  }//- luaEnqueue_pipelineEnd

  /**
   * Add an event to a queue.
   * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
   * 
   * If pipeline is specified, the pipeline definition is used to determine the nodegroup,
   * and the pipeline definition will be appended to the reply when the event is dequeued.
   * 
   * @param {string} queueType in | out | admin
   * @param {string} pipeline If provided, we'll get the nodeGroup from the pipelne
   * @param {string} nodeGroup
   * @param {object} event
   */
  //ZZZZZ Not implemented yet.
   async luaEnqueue_continue(queueType, txState, event, pipeline, checkExternalIdIsUnique, nodeGroup=null) {
    // if (FLOW_VERBOSE)
    console.log(`----- luaEnqueue_continue(queueType=${queueType}, txState, event, pipeline=${pipeline}, checkExternalIdIsUnique=${checkExternalIdIsUnique}, nodeGroup=${nodeGroup})`.brightYellow)
    console.log(`txState=${txState.pretty()}`.magenta)

    let externalIdKey = ''
    if (checkExternalIdIsUnique) {
      // console.log(`NEED TO CHECK EXTERNALID IS UNIQUE`.magenta)
      const owner = txState.getOwner()
      const externalId = txState.getExternalId()
      externalIdKey = `${KEYSPACE_EXTERNALID}${owner}:::${externalId}` // If set, we check it's unique and save the mapping
      // console.log(`externalIdKey=`, externalIdKey)
    }

    if (FLOW_PARANOID) {
      switch (queueType) {
        case 'in':
          validateStandardObject('luaEnqueue_continue() event', event, DEFINITION_PROCESS_STEP_START_EVENT)
          break

        case 'out':
          validateStandardObject('luaEnqueue_continue() event', event, DEFINITION_STEP_COMPLETE_EVENT)
          break
      }
      assert(txState instanceof Transaction)
      txState.vog_validateSteps()
    }

    // Must specify either pipeline or a nodeFroup
    assert(pipeline || nodeGroup)
    assert( !(pipeline && nodeGroup))

    await RedisLua._checkLoaded()

    const eventType = event.eventType

    const txId = txState.getTxId()
    const owner = txState.getOwner()
    txState.vog_setStatusToQueued()

    const metadata = txState.vog_getMetadata()
    const webhook = (metadata.webhook) ? metadata.webhook : ''

    // Save the txState and the event, and perhaps the webhook
    let queueKey
    switch (queueType) {
      case 'in':
        queueKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`
        break
      case 'out':
        queueKey = `${KEYSPACE_QUEUE_OUT}${nodeGroup}`
        break
      case 'admin':
        queueKey = `${KEYSPACE_QUEUE_ADMIN}${nodeGroup}`
        break
    }

// console.log(`event=`.magenta, event)
    const tmpTxState = event.txState
    delete event.txState //ZZZ Should not be set
    const eventJSON = JSON.stringify(event)
    event.txState = tmpTxState
// console.log(`eventJSON=`.magenta, eventJSON)

    // Is this the initial transaction?
    const txStateObject = txState.asObject()
    const isInitialTransaction = (txStateObject.f2.length <= 3)
    // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


    const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const runningKey = `${KEYSPACE_RUNNING}`

    const transactionOutputJSON = txState.getTransactionOutputAsJSON()

    const { stateJSON, externalisedStateFields } = externalizeTransactionState(txState)
    // console.log(`stateJSON=`, stateJSON)
    // console.log(`externalisedStateFields=`, externalisedStateFields)

    // // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
    // const externalisedStateFields = [ ]
    // let txStateObject = txState.asObject()

    // // Extract the externalized fields from the transaction state.
    // // If we delete them then the stateJSON is a little bit smaller, but we
    // // have a problem because other code might still be using this state object.
    // // So, we have three options:

    // // Option 1. Don't delete the externalized values.
    // // const PUT_VALUES_BACK = false
    // // const doDelete = false

    // // Option 2. Use a copy of the transaction state.
    // const PUT_VALUES_BACK = false
    // txStateObject = deepCopy(txStateObject)

    // // Option 3. Put the values back later
    // // const PUT_VALUES_BACK = true
    // // const doDelete = true

    // for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
    //   const value = externaliseStateField_extractValue(txStateObject, map.txState)
    //   // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
    //   externalisedStateFields.push(value)
    // }


    // console.log(`externalisedStateFields=`, externalisedStateFields)
    let result
    try {
      result = await connection_enqueue.datp_enqueue(
          // Keys
          pipelineKey, queueKey, stateKey, runningKey, externalIdKey,
          // Parameters
          queueType, pipeline, nodeGroup,
          eventJSON, stateJSON, transactionOutputJSON,
          owner, txId, webhook,
          eventType,
          isInitialTransaction ? 'true' : 'false',
          externalisedStateFields)
      // console.log(`datp_enqueue returned`, result)

      switch (result[0]) {
        case 'duplicate':
          throw new Error(`CANNOT CREATE A DUPLICATE TRANSACTION`)
  
        case 'error':
          const msg = `${result[1]}: ${result[2]}: ${result[3]}`
          console.log(msg)
          throw new Error(msg)
  
        case 'ok':
          const log = result[6]
          if (SHOWLOG) console.log(`datp_enqueue =>`, log)
          const reply = {
            queue: result[1],
            nodeGroup: result[2],
            pipelineName: result[3],
            pipelineVersion: result[4],
            webhook: result[5] ? result[5] : null
          }

          // Put the extracted values back in (if required)
          de_externalizeTransactionState(txState, externalisedStateFields, 0)
          // console.log(`State afterwards=${txState.stringify()}`)
          return reply
      }
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_enqueue]`)
      throw e
    }
  }//- luaEnqueue_continue

  async findTransactions(filter, statusList) {
    // console.log(`RedisLua.findTransactions(filter=${filter}, statusList=${statusList})`)

    let result
    try {
      result = await connection_enqueue.datp_findTransactions(filter, statusList)
      // console.log(`datp_findTransactions(): result=`, result)

      const txList = [ ]
      for (const row of result) {
        const txId = row[0]
        const externalId = row[1]
        const transactionType = row[2]
        const status = row[3]
        const retry_wakeSwitch = row[4]
        const startTime = row[5]
        const lastUpdated = row[6]
        txList.push({
          txId,
          externalId,
          transactionType,
          status,
          switches: retry_wakeSwitch,
          startTime,
          lastUpdated
        })
      }
      // console.log(`txList=`, txList)

      return txList
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_findTransactions]`)
      throw e
    }
  }//- findTransactions

  /**
   * Get the status of a transaction.
   * 
   * @param {string} txId 
   * @returns 
   */
  async getState(txId, markAsReplied=false, cancelWebhook=false) {
    if (VERBOSE) console.log(`----- getState(${txId}, markAsReplied=${markAsReplied}, cancelWebhook=${cancelWebhook}))`.brightYellow)
    await RedisLua._checkLoaded()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const webhookKey = KEYSPACE_SCHEDULED_WEBHOOK

    let result
    try {
      result = await connection_enqueue.datp_getState(stateKey, webhookKey, markAsReplied, cancelWebhook)
      // console.log(`getState(): result=`, result)
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_getState]`)
      throw e
    }


    // 'txId', 'status', 'event', 'state',
    // console.log(`txId=`, result[0])
    // console.log(`status=`, result[1])
    // console.log(`event=`, result[2])
    // console.log(`state=`, result[3])

    // 'txId', 'status', 'event', 'state', 'eventType', 'pipeline', 'ts')

    const txId2 = result[0]
    if (txId2 === null) {
      // Not found
      console.trace(`Unknown transaction state ${txId2}`)
      return null
    }
    const status = result[1]
    const eventJSON = result[2]
    const stateJSON = result[3]
    // const eventType = result[4]
    // const pipeline = result[5]
    // const ts = parseInt(result[6])
    // const queue = result[7]
    const firstExternalizedFieldIndex = 4

    const txState = transactionStateFromJsonAndExternalizedFields(stateJSON, result)
    // const txState = new TransactionState(stateJSON)
    // // const txStateObject = JSON.parse(stateJSON)
    // const txStateObject = txState.asObject()
    // // console.log(`txStateObject BEFORE =`.brightRed, txStateObject)
    // for (const [i, map] of STATE_TO_REDIS_EXTERNALIZATION_MAPPING.entries()) {
    //   // console.log(`${map.txState} <== ${i} ${map.redis} = ${result[4 + i]}    (${typeof(result[4 + i])})`)
    //   const value = result[firstExternalizedFieldIndex + i]
    //   externaliseStateField_setValue(txStateObject, map.txState, value)
    // }

    // console.log(`txStateObject AFTER =`.brightRed, txStateObject)
    const log = result[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length]
    if (SHOWLOG) console.log(`datp_getState =>`, log)



    // console.log(`stateJSON=`, stateJSON)
    // console.log(`status=`, status)

    const reply = {
      txId,
      txState,
      // pipeline,
      // ts,
      // queue
    }
    if (eventJSON) {
      const event = eventJSON ? JSON.parse(eventJSON) : null
      reply.event = event
    }

    return reply
  }//- getState


  /**
   * Get the status of a transaction.
   * 
   * @param {string} txId 
   * @returns 
   */
   async getMetrics(owner=null, pipeline=null, nodeGroup=null, series='1') {
    if (VERBOSE) console.log(`----- getMetrics(${txId}, markAsReplied=${markAsReplied}, cancelWebhook=${cancelWebhook}))`.brightYellow)
    await RedisLua._checkLoaded()
    // const stateKey = `${KEYSPACE_STATE}${txId}`
    // const webhookKey = KEYSPACE_SCHEDULED_WEBHOOK

    // const METRICS_COMBINED = 1
    // const METRICS_COMBINED_PLUS = 1
    // const METRICS_COMBINED = 1
    // const METRICS_COMBINED = 1
    // const METRICS_COMBINED = 1

    let prefix = ''
    if (owner && pipeline) {
      prefix = `o:::${owner}:::p:::${pipeline}`
    } else if (owner) {
      prefix = `o:::${owner}`
    } else if (pipeline) {
      prefix = `p:::${pipeline}`
    } else if (nodeGroup) {
      prefix = `g:::${nodeGroup}`
    }
    console.log(`prefix=`, prefix)

    let result
    try {
      const withExtras = 1
      series = `${series}` // must be a string
      result = await connection_enqueue.datp_metrics(prefix, withExtras, series)
      console.log(`datp_metrics LUA: result=`, result)
      return result
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_metrics]`)
      throw e
    }
  }//- getMetrics


  async keys(pattern) {
    await RedisLua._checkLoaded()
    return await connection_enqueue.keys(pattern)
  }

  /**
   * Update the status of a transaction the will proceed no further.
   * 
   * @param {string} txId 
   * @param {string} status success | failed | aborted | internal-error
   * @returns 
   */
  async luaTransactionCompleted(tx) {
    // if (VERBOSE) console.log(`luaTransactionCompleted(${tx.getTxId()})`)
    assert (typeof(tx) === 'object')

    const txId = tx.getTxId()
    const status = tx.getStatus()
    const metadata = tx.vog_getMetadata()
    const webhook = tx.vog_getWebhook()
    if (FLOW_VERBOSE) flow2Msg(tx, `----- luaTransactionCompleted(${txId}, ${status}, ${webhook})`)
    // console.log(`tx.pretty()=`, tx.pretty())

    if (status !== 'success' && status!=='failed' && status!=='aborted' && status!=='internal-error') {
      throw new Error('Invalid completion status')
    }

    // Pull some of the fields out of the state object and store them as separate fields in the REDIS hash.
    // See README-txState-in-REDIS.md
    const externalisedStateFields = [ ]
    const txStateObject = tx.asObject()
    const doDelete = false
    for (const map of STATE_TO_REDIS_EXTERNALIZATION_MAPPING) {
      const value = externaliseStateField_extractValue(txStateObject, map.txState, doDelete)
      // console.log(`=> ${map.txState} = ${value}   (${typeof(value)})`)
      externalisedStateFields.push(value)
    }


    const stateJSON = tx.stringify()
    assert(txId)
    assert(status)
    assert(stateJSON)

    // Call the LUA script
    await RedisLua._checkLoaded()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const webhookKey = requiresWebhookReply(metadata) ? `${KEYSPACE_SCHEDULED_WEBHOOK}` : null
    const runningKey = `${KEYSPACE_RUNNING}`
    const persistKey = `${KEYSPACE_TOARCHIVE}`

    // console.log(`webhookKey=`, webhookKey)
    try {
      const result = await connection_enqueue.datp_completeTransaction(stateKey, webhookKey, runningKey, persistKey, txId, status, stateJSON, externalisedStateFields)
      const log = result[3]
      if (SHOWLOG) console.log(`datp_completeTransaction =>`, log)
      return true
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_completeTransaction]`)
      throw e
    }
  }

  async deleteZZZ(txId) {
    console.log(`deleteZZZ(${txId})`)
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const result = await connection_enqueue.hdel(stateKey, 'txId', 'status', 'ts', 'state', 'event', 'pipeline', 'eventType', 'queue')
    // console.log(`result=`, result)
  }

  /**
   * Get events from the queues for a nodeGroup.
   * 
   * @param {string} nodeGroup 
   * @param {number} numEvents 
   * @returns 
   */
  async luaDequeue(nodeGroup, numEvents) {
    

    await RedisLua._checkLoaded()
    const qInKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`
    const qOutKey = `${KEYSPACE_QUEUE_OUT}${nodeGroup}`
    const qAdminKey = `${KEYSPACE_QUEUE_ADMIN}${nodeGroup}`
    const runningKey = `${KEYSPACE_RUNNING}`
    const pipelinesKey = `${KEYSPACE_PIPELINE}`
    // console.log(`getting ${numEvents} events`)

    // console.log(`CALLING datp_dequeue`)
    // if (nodeGroup === 'slave1') {
    // console.log(`----- luaDequeue(${nodeGroup}, ${numEvents})`)
    // console.log(`qInKey=`, qInKey)
    //   console.log(`qOutKey=`, qOutKey)
    //   console.log(`qAdminKey=`, qAdminKey)
    //   console.log(`runningKey=`, runningKey)
    //   console.log(`pipelinesKey=`, pipelinesKey)
    // }

    let result
    try {
      result = await connection_enqueue.datp_dequeue(qInKey, qOutKey, qAdminKey, runningKey, pipelinesKey, nodeGroup, numEvents)
      if (result.length > 1) { // Always have log
        if (FLOW_VERBOSE) console.log(`----- luaDequeue(${nodeGroup}, ${numEvents})`.gray)
        // console.log(`result=`, result)
      }
      // if (result.length > 1) { // Always have log
      //   console.log(`----- luaDequeue(${nodeGroup}, ${numEvents})`.brightYellow)
      // }
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_dequeue]`)
      throw e
    }

    // Get the events from the reply
    const events = [ ]
    for (const values of result) {
      switch (values[0]) {
        case 'warning':
          // A error occurred. Non-fatal, but report it.
          console.log(`Warning ${values[1]}: ${values[2]}: ${values[3]}`)
          console.log(``)
          break

        case 'event':
          // This is a valid event
          const txStateValues = values[1]
          // console.log(`txStateValues=`, txStateValues)

          // Get the txState details selected from REDIS
          const owner = txStateValues[0]
          const status = txStateValues[1]
          const eventJSON = txStateValues[2]
          // console.log(`eventJSON=`, eventJSON)
          const stateJSON = txStateValues[3]
          // console.log(`stateJSON=`, stateJSON)
          const pipelineName = txStateValues[4]
          const firstExternalizedFieldIndex = 5
          // Then the externalised fields
          const pipelineDefinitionJSON  = txStateValues[firstExternalizedFieldIndex + STATE_TO_REDIS_EXTERNALIZATION_MAPPING.length]
          // console.log(`pipelineDefinitionJSON=`, pipelineDefinitionJSON)

          // Create the event
          const event = JSON.parse(eventJSON)

          // Create the transaction state.
          const txState = transactionStateFromJsonAndExternalizedFields(stateJSON, txStateValues)

          // // We need to provide fake txId, owner, externalId and transactionType so it
          // // can be created, because these have been removed from the JSON. Those values
          // // will be patched back in below, along with the other externalized fields.
          // const txState = new TransactionState(stateJSON)
          // const txStateObject = txState.asObject()
          // for (const [i, map] of STATE_TO_REDIS_EXTERNALIZATION_MAPPING.entries()) {
          //   const value = txStateValues[firstExternalizedFieldIndex + i]
          //   // console.log(`${map.txState} <== ${i} ${value}`)
          //   externaliseStateField_setValue(txStateObject, map.txState, value)
          // }

          // Update the status. It has already been updated in REDIS,
          // but we selected the txState details before it was updated.
          txState._patchInStatus(TX_STATUS_RUNNING)
          // console.log(`txState=`, txState.pretty())

          // A few sanity checks
          assert(event.f2i >= 0)
          const stepId = txState.vf2_getStepId(event.f2i)

          // Patch in the pipeline definition.
          if (stepId && pipelineDefinitionJSON && pipelineDefinitionJSON !== '-') {
            // console.log(`PATCHING IN STEP DEFINITION`)
            const step = txState.stepData(stepId)
            assert(step)
              // step.vogStepDefinition = {
            step.stepDefinition = {
              stepType: STEP_TYPE_PIPELINE,
              description: `Pipeline ${pipelineName}`,
              steps: JSON.parse(pipelineDefinitionJSON)
            }
            if (FLOW_PARANOID) {
              validateStandardObject('redis-lua.luaDequeue() step', step, STEP_DEFINITION)
            }
          }

          if (FLOW_PARANOID) {
            // console.log(`event.eventType=`, event.eventType)
            switch (event.eventType) {
              case Scheduler2.STEP_START_EVENT:
                validateStandardObject('redis-lua.luaDequeue() step start event', event, DEFINITION_PROCESS_STEP_START_EVENT)
                break

              case Scheduler2.STEP_COMPLETED_EVENT:
                validateStandardObject('redis-lua.luaDequeue() step completed event', event, DEFINITION_STEP_COMPLETE_EVENT)
                break

            }
            // validateStandardObject('redis-lua.luaDequeue() flow', flow, FLOW_DEFINITION)
            // validateStandardObject('redis-lua.luaDequeue() step', step, STEP_DEFINITION)
          }

          // Add the event and transaction state to our reply.
          events.push({ txState, event })
          break

        case 'log':
          // console.log(`log=`, values[1])
      }//- switch
    }//- for

    // if (events.length > 0 && FLOW_VERBOSE) {
    //   console.log(`luaDequeue(): dequeued ${events.length} events`)
    // }
    return events
  }//- luaDequeue


  
  /**
   * Get a list of transaction states that need to be persisted.
   * After the details have been persisted, this function should be called again with
   * 'persistedTransactionIds' so the transactions are removed from the list of
   *  transactions needing to be archived.
   * 
   * @param {Array<string>} persistedTransactionIds IDs of transactions that have been persisted
   * @param {string} nodeId ID of node offerring to persist transactions
   * @param {Integer} numRequired Number of transaction IDs to persist
   * @returns Array of transaction details
   */
  async transactionsToArchive(persistedTransactionIds, nodeId, numRequired) {
    // if (yarpCnt++ === 0) console.log(`NOT ARCHIVING`.bgMagenta)
    // if (persistedTransactionIds.length > 0) console.log(`persistedTransactionIds=`, persistedTransactionIds)
    // return [ ]

    // console.log(`----- transactionsToArchive(persistedTransactionIds, ${nodeId}, ${numRequired})`, persistedTransactionIds)
    await RedisLua._checkLoaded()
    const persistKey = `${KEYSPACE_TOARCHIVE}`
    const persistLockKey1 = `${KEYSPACE_TOARCHIVE_LOCK1}`
    const persistLockKey2 = `${KEYSPACE_TOARCHIVE_LOCK2}`
    try {
      const result = await connection_enqueue.datp_transactionsToArchive(persistKey, persistLockKey1, persistLockKey2, nodeId, numRequired, persistedTransactionIds.length, persistedTransactionIds)
      // if (result.length > 0) {
      //   console.log(`transactionsToArchive result=`.bgMagenta, result)
      // }
      return result
    } catch (e) {
      console.log(`FATAL ERROR calling LUA script [datp_transactionsToArchive]`)
      throw e
    }
  }


  /**
   * Get a list of transaction states that need to be persisted.
   * After the details have been persisted, this function should be called again with
   * 'persistedTransactionIds' so the transactions are removed from the list of
   *  transactions needing to be archived.
   * 
   * @param {Array<string>} persistedTransactionIds IDs of transactions that have been persisted
   * @param {string} nodeId ID of node offerring to persist transactions
   * @param {Integer} numRequired Number of transaction IDs to persist
   * @returns Array of transaction details
   */
  async getWebhooksToProcess(webhookResults, numRequired) {
    if (VERBOSE && webhookResults.length > 0) {
      console.log(`----- getWebhooksToProcess(webhookResults, ${numRequired})`)
      console.log(`webhookResults=`, webhookResults)
    }
// console.log(`getWebhooksToProcess() - RETURNING NO WEBHOOKS`.brightRed)
//     return []

    const txIdsAndStatuses = [ ]
    for (const result of webhookResults) {
      if (typeof(result.txId) === 'undefined') {
        throw new Error('Missing result.txId')
      }
      if (typeof(result.result) === 'undefined') {
        throw new Error('Missing result.result')
      }
      if (typeof(result.comment) === 'undefined') {
        throw new Error('Missing result.comment')
      }
      txIdsAndStatuses.push(result.txId)
      txIdsAndStatuses.push(result.result)
      txIdsAndStatuses.push(result.comment)
    }

    await RedisLua._checkLoaded()
    const webhookListKey = `${KEYSPACE_SCHEDULED_WEBHOOK}`
    try {
      // if (txIdsAndStatuses.length > 0) {
      //   console.log(`txIdsAndStatuses=`, txIdsAndStatuses)
      // }
      let result
      try {
        result = await connection_enqueue.datp_webhooksToProcess(webhookListKey, numRequired, webhookResults.length, txIdsAndStatuses)
        // if (result.length > 0) {
        //   console.log(`datp_webhooksToProcess returned`, result)
        // }
      } catch (e) {
        console.log(`FATAL ERROR calling LUA script [datp_webhooksToProcess]`)
        throw e
      }
  
      const batch = [ ]
      for (const values of result) {
        switch (values[0]) {
          case 'warning':
            // A error occurred. Non-fatal, but report it.
            console.log(`Warning ${values[1]}: ${values[2]}: ${values[3]}`)
            console.log(``)
            break

          case 'webhook':
            // console.log(`=========> `, values)
            const txId = values[1]
            const state = values[2]
            const webhook = values[3]
            const retryCount = values[4]
            const webhookType = values[5]
            const completionTime = values[6]
            const retryTime = values[7]
            const nextRetryTime = values[8]
            batch.push({ txId, state, webhook, retryCount, webhookType, completionTime, retryTime, nextRetryTime })
            break
        }
      }
      // console.log(`getWebhooksToProcess() finished`)
      return batch
    } catch (e) {
      console.log(`e=`, e)
    }
  }

  // async transactionSummaryToReturn(txId, cancelWebhook) {

  //   let result
  //   try {
  //     await RedisLua._checkLoaded()
  //     const txId = txState.getTxId()
  //     // const metadata = txState.vog_getMetadata()
  //     // const cancelWebhook = requiresWebhookReply(metadata)
  //     result = await connection_enqueue.datp_transactionResultReturned(txId, cancelWebhook)
  //     console.log(`result=`, result)
  //   } catch (e) {
  //     console.log(`FATAL ERROR calling LUA script [datp_transactionResultReturned]`)
  //     throw e
  //   }
  // }


  /**
   * 
   * @param {*} callback 
   */
  async listenToNotifications(callback) {
    console.log(`----- listenToNotifications()`)
    await RedisLua._checkLoaded()

    connection_admin.subscribe(CHANNEL_NOTIFY, (err, count) => {
      if (err) {
        // Just like other commands, subscribe() can fail for some reasons,
        // ex network issues.
        console.error("Failed to subscribe: %s", err.message);
      } else {
        // `count` represents the number of channels this client are currently subscribed to.
        console.log(
          `Subscribed successfully! This client is currently subscribed to ${count} channels.`
        );
      }
    });
    
    connection_admin.on("message", (channel, message) => {
      // console.log(`Received ${message} from ${channel}`);
      const arr = message.split(':')
      const type = arr[0]
      const txId = arr[1]
      const status = arr[2]
      callback(type, txId, status)
    });
  }//- listenToNotifications

  async notify(txId) {
    connection_admin.publish(CHANNEL_NOTIFY, txId);
    console.log("Published %s to %s", txId, CHANNEL_NOTIFY);
  }//- notify


  /**
   * Select our pipeline definitions from the database and upload them to REDIS.
   */
  async uploadPipelineDefinitions() {
    // console.log(`uploadPipelineDefinitions()`)
    await RedisLua._checkLoaded()

    const sql = `SELECT name, version, node_name AS nodeGroup, steps_json AS definition FROM atp_pipeline WHERE status='active'`
    const params = [ ]
    const pipelines = await query(sql, await params)
    for (const pipeline of pipelines) {
      const key = `${KEYSPACE_PIPELINE}${pipeline.name}`
      const nodeGroup = pipeline.nodeGroup ? pipeline.nodeGroup : 'master'
      await connection_admin.hset(key,
        'version', pipeline.version,
        'nodeGroup', nodeGroup,
        'definition', pipeline.definition
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

    const pipelines = await connection_admin.keys(`${KEYSPACE_PIPELINE}*`)
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
function externalizeTransactionState(txState) {

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
function de_externalizeTransactionState(txState, externalisedStateFields, firstExternalizedFieldIndex) {
  if (externalizationMode === EXTERNALIZE_BUT_DONT_DELETE) {
    // We didn't take the values out, so we don't need to put them back in.
    return
  }

  const txStateObject = txState.asObject()
  // console.log(`txStateObject BEFORE =`.brightRed, txStateObject)
  for (const [i, map] of STATE_TO_REDIS_EXTERNALIZATION_MAPPING.entries()) {
    // console.log(`${map.txState} <== ${i} ${map.redis} = ${externalisedStateFields[firstExternalizedFieldIndex + i]}    (${typeof(fieldValues[firstExternalizedFieldIndex + i])})`)
    const value = externalisedStateFields[firstExternalizedFieldIndex + i]
    externaliseStateField_setValue(txStateObject, map.txState, value)
  }
  // console.log(`txStateObject AFTER =`.brightRed, txStateObject)
}


function transactionStateFromJsonAndExternalizedFields(stateJSON, externalisedStateFields, firstExternalizedFieldIndex) {
  // We need to provide fake txId, owner, externalId and transactionType so it
  // can be created, because these have been removed from the JSON. Those values
  // will be patched back in below, along with the other externalized fields.
  const txState = new TransactionState(stateJSON)
  de_externalizeTransactionState(txState, externalisedStateFields, firstExternalizedFieldIndex)
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


function externaliseStateField_extractValue(object, path, doDelete) {
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
