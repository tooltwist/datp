/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert';
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_SUCCESS, STEP_TIMEOUT } from '../../Step';
import { flow2Msg } from '../flowMsg';
import { requiresWebhookReply, WEBHOOK_RESULT_ABORTED, WEBHOOK_RESULT_FAILED, WEBHOOK_RESULT_SUCCESS, WEBHOOK_STATUS_ABORTED, WEBHOOK_STATUS_DELIVERED, WEBHOOK_STATUS_PENDING, WEBHOOK_STATUS_PROCESSING, WEBHOOK_STATUS_RETRY, WEBHOOK_STATUS_MAX_RETRIES, WEBHOOK_EVENT_TXSTATUS } from '../webhooks/tryTheWebhook';
import { CHANNEL_NOTIFY, DELAY_BEFORE_ARCHIVING, EXTERNALID_UNIQUENESS_PERIOD, externaliseStateField_extractValue, FLOW_VERBOSE, KEYSPACE_EXCEPTION, KEYSPACE_NODE_REGISTRATION, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, KEYSPACE_TOARCHIVE, KEYSPACE_TOARCHIVE_LOCK1, KEYSPACE_TOARCHIVE_LOCK2, MAX_WEBHOOK_RETRIES, NUMBER_OF_EXTERNALIZED_FIELDS, PERSISTING_PERMISSION_LOCK1_PERIOD, PERSISTING_PERMISSION_LOCK2_PERIOD, PROCESSING_STATE_COMPLETE, PROCESSING_STATE_PROCESSING, PROCESSING_STATE_QUEUED, PROCESSING_STATE_SLEEPING, RedisLua, SHOWLOG, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3, VERBOSE, WEBHOOK_EXPONENTIAL_BACKOFF, WEBHOOK_INITIAL_DELAY } from './redis-lua';

export async function registerLuaScripts_txComplete() {
  // console.log(`registerLuaScripts_txComplete() `.magenta)
  const connection = await RedisLua.regularConnection()


    /*
     *  Complete a transaction.
     */
    connection.defineCommand("datp_completeTransaction", {
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
        local existingValues = redis.call('hmget', stateKey, 'processingState', 'webhook')
        local existingStatus = existingValues[1]
        local webhook = existingValues[2]
        if not existingStatus then
          local msg = 'Internal Error: Transaction to be completed has no state saved [' .. txId .. ']'
          return { 'error', 'E662622', txId, msg }
        end
        log[#log+1] = 'Verified that transaction state exists in REDIS'

        -- Check the transaction completion status is valid
        if status ~= '${STEP_ABORTED}' and status ~= '${STEP_FAILED}' and status ~= '${STEP_INTERNAL_ERROR}' and status ~= '${STEP_SUCCESS}' and status ~= '${STEP_TIMEOUT}' then
          local msg = 'Internal Error: Cannot complete transaction with status "' .. status .. '" [' .. txId .. ']'
          return { 'error', 'E686221', txId, msg }
        end
        log[#log+1] = 'New status [' .. status .. '] is valid'

        -- Check the existing processingState allows completion
        if existingStatus ~= '${PROCESSING_STATE_PROCESSING}' then
          local msg = 'Internal Error: Cannot complete transaction with current processingState "' .. existingStatus .. '" [' .. txId .. ']'
          return { 'error', 'E662752', txId, msg }
        end
        log[#log+1] = 'Existing processingState [' .. existingStatus .. '] allows completion to proceed'

        -- What is the current time?
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        log[#log+1] = 'Current time is ' .. now

        -- Update the processingState, state, and remove the latest event
        redis.call('hset', stateKey,
          'processingState', '${PROCESSING_STATE_COMPLETE}',
          'state', state,
          -- not sleeping
          'retry_sleepingSince', 0,
          'retry_counter', 0,
          'zzzzzz_counterReset_3', 123,
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

        -- Remove it from the 'processing' list
        redis.call('zrem', '${KEYSPACE_PROCESSING}', txId)
        log[#log+1] = 'Remove the transaction ID from the "processing list"'

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

}


/**
 * Update the status of a transaction that will proceed no further.
 * 
 * @param {string} txId 
 * @param {string} status success | failed | aborted | internal-error
 * @returns 
 */
export async function luaTransactionCompleted(tx) {
  if (VERBOSE) console.log(`luaTransactionCompleted(${tx.getTxId()})`)
  assert (typeof(tx) === 'object')
  // console.log(`typeof tx=`, typeof tx)
  // console.log(`tx.asObject()=`, tx.asObject())

  const txId = tx.getTxId()
  const status = tx.getStatus()
  // console.log(`luaTransactionCompleted(): txId=`, txId)
  // console.log(`luaTransactionCompleted(): status=`, status)

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
  // assert(txId)
  // assert(status)
  // assert(stateJSON)

  // Call the LUA script
  const stateKey = `${KEYSPACE_STATE}${txId}`
  const webhookKey = requiresWebhookReply(metadata) ? `${KEYSPACE_SCHEDULED_WEBHOOK}` : null
  const runningKey = `${KEYSPACE_PROCESSING}`
  const persistKey = `${KEYSPACE_TOARCHIVE}`

  // console.log(`webhookKey=`, webhookKey)
  try {
    const connection = await RedisLua.regularConnection()
    const result = await connection.datp_completeTransaction(stateKey, webhookKey, runningKey, persistKey, txId, status, stateJSON, externalisedStateFields)
    const log = result[3]
    if (SHOWLOG) console.log(`datp_completeTransaction =>`, log)
    return true
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_completeTransaction]`)
    throw e
  }
}//- luaTransactionCompleted

