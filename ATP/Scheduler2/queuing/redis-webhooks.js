/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { WEBHOOK_RESULT_ABORTED, WEBHOOK_RESULT_FAILED, WEBHOOK_RESULT_SUCCESS, WEBHOOK_STATUS_ABORTED, WEBHOOK_STATUS_DELIVERED, WEBHOOK_STATUS_PROCESSING, WEBHOOK_STATUS_RETRY, WEBHOOK_STATUS_MAX_RETRIES } from '../webhooks/tryTheWebhook';
import { KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_STATE, MAX_WEBHOOK_RETRIES, RedisLua, VERBOSE, WEBHOOK_EXPONENTIAL_BACKOFF, WEBHOOK_INITIAL_DELAY } from './redis-lua';

export async function registerLuaScripts_webhooks() {
  // console.log(`registerLuaScripts_webhooks() `.magenta)
  const connection = await RedisLua.regularConnection()


    /*
     *  Get details of transactions that need to a webhook reply.
     *  - Receives as input the results from procesing the previous batch.
     *  - The sort order of the webhook Z list is the time to next try the webhook.
     *  - At the time we pull elements off the list, we schedule them for retry.
     *  - If we are told in the subsequent call that the webhook succeeded, we
     *    remove it from the list.
     */
    connection.defineCommand("datp_webhooksToProcess", {
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
              -- Event is in list for a webhook, but we don't have a processingState/state!
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
              redis.call('ZREM', webhookListKey, txId)
              redis.call('ZADD', webhookListKey, nextRetryTime, txId)
              redis.call('HSET', stateKey,
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
}//- registerLuaScripts_webhooks



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
export async function luaGetWebhooksToProcess(webhookResults, numRequired) {
  if (VERBOSE && webhookResults.length > 0) {
    console.log(`----- luaGetWebhooksToProcess(webhookResults, ${numRequired})`)
    console.log(`webhookResults=`, webhookResults)
  }
// console.log(`luaGetWebhooksToProcess() - RETURNING NO WEBHOOKS`.brightRed)
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
      const connection = await RedisLua.regularConnection()
      result = await connection.datp_webhooksToProcess(webhookListKey, numRequired, webhookResults.length, txIdsAndStatuses)
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
    // console.log(`luaGetWebhooksToProcess() finished`)
    return batch
  } catch (e) {
    console.log(`e=`, e)
  }
}//- luaGetWebhooksToProcess
