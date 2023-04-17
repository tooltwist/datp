/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { KEYSPACE_PROCESSING, KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, KEYSPACE_TOARCHIVE, NUMBER_OF_EXTERNALIZED_FIELDS, NUMBER_OF_READONLY_FIELDS, RedisLua, STATE_TO_REDIS_READONLY_MAPPING, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, transactionStateFromJsonAndExternalizedFields, VERBOSE } from './redis-lua';

export async function registerLuaScripts_cachedState() {
  // console.log(`registerLuaScripts_cachedState() `.magenta)
  const connection = await RedisLua.regularConnection()


    /*
     *  Get a transaction state.
     */
    connection.defineCommand("datp_getCachedState", {
      numberOfKeys: 1,
      lua: `
      local stateKey = KEYS[1]
      local txId = ARGV[1]
      local withMondatDetails = ARGV[2]
      local markAsReplied = ARGV[3]
      local cancelWebhook = ARGV[4]
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
        'processingState',
        'event',
        'state',
        --'eventType', 'pipeline',
        'nodeGroup',
        'pipeline',
        'queue',
        'ts',
        -- 'queue', extras)
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
        '${STATE_TO_REDIS_READONLY_MAPPING[0].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[1].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[2].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[3].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[4].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[5].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[6].redis}',
        '${STATE_TO_REDIS_READONLY_MAPPING[7].redis}'
      )
      if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 16 then
        return { 'error', 'Internal error #992822: Incorrect number of externalised parameters' }
      end
      if ${NUMBER_OF_READONLY_FIELDS} ~= 8 then
        return { 'error', 'Internal error #992407: Incorrect number of readonly parameters' }
      end




      log[#log+1] = 'Selected JSON and externalized fields for the transaction state'

      -- Perhaps see which lists this is in
      local inProcessingList = nil
      local toArchive = nil
      local inWebhookList = nil
      local inSleepingList = nil
      local ttl = nil
      inProcessingList = redis.call('ZSCORE', "${KEYSPACE_PROCESSING}", txId)
      inSleepingList = redis.call('ZSCORE', "${KEYSPACE_SLEEPING}", txId)
      inWebhookList = redis.call('ZSCORE', "${KEYSPACE_SCHEDULED_WEBHOOK}", txId)
      toArchive = redis.call('ZSCORE', "${KEYSPACE_TOARCHIVE}", txId)
      ttl = redis.call('TTL', stateKey)

      -- Perhaps cancel the webhook
      -- This only happens if we specify that polling success
      local txId = fields[1]
      if txId then
        if cancelWebhook == 'true' then
          redis.call('zrem', '${KEYSPACE_SCHEDULED_WEBHOOK}', txId)
          --redis.call('set', '@cancelled-webhook-' .. txId, 'yep')
          log[#log+1] = 'Cancelled the webhook'
          end
      end
      fields[#fields+1] = inProcessingList
      fields[#fields+1] = inSleepingList
      fields[#fields+1] = inWebhookList
      fields[#fields+1] = toArchive
      fields[#fields+1] = ttl

      fields[#fields+1] = log
      return fields
    `})//- datp_getCachedState

}


/**
 * Get the state of a transaction.
 * 
 * @param {string} txId 
 * @returns 
 */
export async function luaGetCachedState(txId, withMondatDetails=false, markAsReplied=false, cancelWebhook=false) {
  if (VERBOSE) console.log(`----- luaGetCachedState(${txId}, withMondatDetails=${withMondatDetails}, markAsReplied=${markAsReplied}, cancelWebhook=${cancelWebhook}))`.yellow)
  const stateKey = `${KEYSPACE_STATE}${txId}`

  let result
  try {
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_getCachedState(stateKey, txId, withMondatDetails, markAsReplied, cancelWebhook)
    // if (result.length > 0 && result[0] !== null) {
    // console.log(`luaGetCachedState(): result=`, result)
    // }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_getCachedState]`)
    throw e
  }

  const txId2 = result[0]
  const processingState = result[1]
  const eventJSON = result[2]
  const stateJSON = result[3]
  const nodeGroup = result[4]
  const pipeline = result[5]
  const queue = result[6]
  let ts = parseInt(result[7])
  // const queue = result[7]
  const firstExternalizedFieldIndex = 8
  const firstReadonlyFieldIndex = firstExternalizedFieldIndex + NUMBER_OF_EXTERNALIZED_FIELDS
  const extraFieldsIndex = firstReadonlyFieldIndex + NUMBER_OF_READONLY_FIELDS
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
  let inProcessingList = result[extraFieldsIndex + 0]
  let inSleepingList = result[extraFieldsIndex + 1]
  let inWebhookList = result[extraFieldsIndex + 2]
  let toArchive = result[extraFieldsIndex + 3]
  let ttl = result[extraFieldsIndex + 4]
  // const log = result[extraFieldsIndex + 5]
  // if (SHOWLOG) console.log(`datp_getCachedState =>`, log)
// console.log(`inProcessingList=`, inProcessingList)
// console.log(`inSleepingList=`, inSleepingList)

  inProcessingList = parseInt(inProcessingList)
  toArchive = parseInt(toArchive)
  inWebhookList = parseInt(inWebhookList)
  inSleepingList = parseInt(inSleepingList)
  ttl = parseInt(ttl)
  ts = parseInt(ts)

  // if (txId2 === null) {
  //   // Not found
  //   console.log(`Transaction state not cached - checking the archives (${txId})`)
  //   // const state = await getArchivedTransactionState(txId)
  //   // if (state === null) {
  //   //   console.trace(`Transaction state not cached (txId=${txId})`)
  //   // }
  //   return null
  // }
  if (txId2 === null) {
    // Not in the cache
    const reply = {
      cached: false,
      processingState,
      inProcessingList,
      toArchive,
      inWebhookList,
      inSleepingList,
      nodeGroup,
      pipeline,
      queue,
      ttl,
      ts,
    }
    return reply
  } else {

    // Was found in the cache
    if (VERBOSE) {
      console.log(`Found transaction state in the cache`)
    }
    // console.log(`stateJSON=`, stateJSON)
    // console.log(`processingState=`, processingState)


    const txState = transactionStateFromJsonAndExternalizedFields(stateJSON, result, firstExternalizedFieldIndex, firstReadonlyFieldIndex)
    // console.log(`\n\ntxState=`.red, txState.asObject())
    const reply = {
      cached: true,
      txId,
      txState,
      processingState,
      inProcessingList,
      toArchive,
      inWebhookList,
      inSleepingList,
      nodeGroup,
      pipeline,
      queue,
      ttl,
      ts,
      // queue
    }
    if (eventJSON) {
      const event = eventJSON ? JSON.parse(eventJSON) : null
      reply.event = event
    }

    // console.log(`WIBBLE reply=`, reply)

    return reply
  }
}//- luaGetCachedState
