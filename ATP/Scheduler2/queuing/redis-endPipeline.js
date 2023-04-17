/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert';
import { DEFINITION_STEP_COMPLETE_EVENT, validateStandardObject } from '../eventValidation';
import Transaction from '../TransactionState';
import { de_externalizeTransactionState, EXTERNALID_UNIQUENESS_PERIOD, externalizeTransactionState, FLOW_PARANOID, FLOW_VERBOSE, KEYSPACE_PIPELINE, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, NUMBER_OF_EXTERNALIZED_FIELDS, PROCESSING_STATE_QUEUED, RedisLua, SHOWLOG, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3 } from './redis-lua';



export async function registerLuaScripts_endPipeline() {
  // console.log(`registerLuaScripts_endPipeline() `.magenta)
  const connection = await RedisLua.regularConnection()

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
  connection.defineCommand("datp_endPipeline", {
    numberOfKeys: 1,
    lua: `
      local stateKey = KEYS[1]

      -- arguments to this function
      local operation = ARGV[1]
      local nodeGroup = ARGV[2]
      local eventJSON = ARGV[3]
      local stateJSON = ARGV[4]
      local txOutputJSON = ARGV[5]
      local owner = ARGV[6]
      local txId = ARGV[7]
      local webhook = ARGV[8]
      local eventType = ARGV[9]

      -- variables
      local INDEX_OF_EXTERNALIZED_FIELDS = 10
      local log = { }

      -- 2. Check this transaction does not already have queued status
      local processingState = redis.call('hget', stateKey, 'processingState')
      if processingState == '${PROCESSING_STATE_QUEUED}' then
        local msg = 'Internal Error: datp_endPipeline: Transaction is already queued'
        return { 'error', 'E99288', txId, msg }
      end
      log[#log+1] = 'Checked transaction does not already have queued status'


      -- 4. Determine the queue key
      local queueKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup

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
        'ts', now,
        'queue', queueKey,
        -- not sleeping
        'zzzzzz_counterReset_2', 456,
        'retry_sleepingSince', 0,
        'retry_counter', 500,
        'retry_wakeTime', 0,
        'retry_wakeSwitch', '',

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
        return { 'error', 'E9626322', 'datp_endPipeline', 'Incorrect number of parameters' }
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
      local f_ownerStart = 'o:::' .. owner .. ':::queued'
      local f_qInLength = 'iLen:::' .. nodeGroup
      local f_qOutLength = 'oLen:::' .. nodeGroup

      -- Short interval
      local timeslot1 = math.floor(seconds / ${STATS_INTERVAL_1}) * ${STATS_INTERVAL_1}
      local expiry1 = timeslot1 + ${STATS_RETENTION_1}
      local statsKey1 = '${KEYSPACE_STATS_1}' .. timeslot1
      redis.call('hincrby', statsKey1, f_start, 1)
      redis.call('hincrby', statsKey1, f_ownerStart, 1)
      redis.call('hincrby', statsKey1, f_nodeGroup, 1)
      redis.call('hincrby', statsKey1, f_txStart, 1)
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
      redis.call('hset', statsKey3, f_qInLength, inputLength)
      redis.call('hset', statsKey3, f_qOutLength, outputLength)
      redis.call('expireat', statsKey3, expiry3)
      log[#log+1] = 'Updated the metrics collection'
      
      return { 'ok', queueKey, nodeGroup, webhook, log }
    `
  });//- datp_endPipeline
}


/**
 * Add an end-of-pipeline event to a queue.
 * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
 * 
 * @param {TransactionState} txState
 * @param {string} nodeGroup
 * @param {object} event
 */
export async function luaEnqueue_pipelineEnd(txState, event, nodeGroup) {
  if (FLOW_VERBOSE) console.log(`----- luaEnqueue_pipelineEnd(txState, event, nodeGroup=${nodeGroup}, checkExternalIdIsUnique=${checkExternalIdIsUnique})`.gray)
  // console.log(`txState.getTxId()=`, txState.getTxId())

  if (FLOW_PARANOID) {
    validateStandardObject('luaEnqueue_pipelineEnd() event', event, DEFINITION_STEP_COMPLETE_EVENT)
    assert(txState instanceof Transaction)
    txState.vog_validateSteps()
  }

  assert(nodeGroup)

  await RedisLua._checkLoaded()

  const eventType = event.eventType

  const txId = txState.getTxId()
  const owner = txState.getOwner()
  // txState.vog_setStatusToQueued()

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
  //ZZZ Isn't this only for pipeline start?
  const txStateObject = txState.asObject()
  const isInitialTransaction = false
  // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


  // const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
  const stateKey = `${KEYSPACE_STATE}${txId}`
  const runningKey = `${KEYSPACE_PROCESSING}`

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
    console.log(`Calling datp_endPipeline from luaEnqueue_pipelineEnd`.magenta)
    
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_endPipeline(
        // Keys
        stateKey,
        // Parameters
        'end-pipeline',
        nodeGroup,
        eventJSON, stateJSON, transactionOutputJSON,
        owner, txId, webhook,
        eventType,
        externalisedStateFields)

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
          webhook: result[3] ? result[3] : null
        }

        // Put the extracted values back in (if required)
        // de_externalizeTransactionState(txState, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, externalisedStateFields, 0)
        // console.log(`State afterwards=${txState.stringify()}`)
        return reply
    }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_enqueue]`)
    throw e
  }
}//- luaEnqueue_pipelineEnd
