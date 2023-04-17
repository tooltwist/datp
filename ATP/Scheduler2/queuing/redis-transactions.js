/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { CHANNEL_NOTIFY, DELAY_BEFORE_ARCHIVING, EXTERNALID_UNIQUENESS_PERIOD, KEYSPACE_EXCEPTION, KEYSPACE_NODE_REGISTRATION, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, KEYSPACE_TOARCHIVE, KEYSPACE_TOARCHIVE_LOCK1, KEYSPACE_TOARCHIVE_LOCK2, MAX_WEBHOOK_RETRIES, NUMBER_OF_EXTERNALIZED_FIELDS, NUMBER_OF_READONLY_FIELDS, PERSISTING_PERMISSION_LOCK1_PERIOD, PERSISTING_PERMISSION_LOCK2_PERIOD, PROCESSING_STATE_COMPLETE, PROCESSING_STATE_PROCESSING, PROCESSING_STATE_QUEUED, PROCESSING_STATE_SLEEPING, RedisLua, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATE_TO_REDIS_READONLY_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3, WEBHOOK_EXPONENTIAL_BACKOFF, WEBHOOK_INITIAL_DELAY } from './redis-lua';

export async function registerLuaScripts_transactions() {
  // console.log(`registerLuaScripts_transactions() `.magenta)
  const connection = await RedisLua.regularConnection()


    /*
     *  Get a transaction state.
     */
    connection.defineCommand("datp_findTransactions", {
      numberOfKeys: 0,
      lua: `
      local filter = KEYS[1]

      if 1 == 2 then
        return { 'error', 'I dun lyke Mondese yarp ZZZZZ' }
      end

      -- ZZZZZ Use SCAN instead
      -- This is horrible.
      -- We need to look at EVERY key to find the transaction status keys
      local rv = redis.call('scan', '0', 'MATCH', '${KEYSPACE_STATE}*', 'COUNT', 500)
      if 1 == 2 then
        return rv
      end
      --local keys = redis.call('keys', '${KEYSPACE_STATE}*')
      local keys = rv[2]



      local result = { }
      for i = 1, #keys do
        local state = redis.call('hmget', keys[i], 'txId', 'externalId', 'transactionType', 'processingState', 'retry_wakeSwitch', 'startTime', 'lastUpdated')
        local processingState = state[4]
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
        'owner',
        'externalId',
        'transactionType',
        'processingState',
        'retry_wakeTime',
        'retry_wakeSwitch',
        'startTime',
        'lastUpdated'
      )
      -- ZZZZZ Do we need to get the externalized fields?
      if ${NUMBER_OF_EXTERNALIZED_FIELDS} ~= 16 then
        return { 'error', 'Internal error #992822: Incorrect number of parameters' }
      end
      if ${NUMBER_OF_READONLY_FIELDS} ~= 8 then
        return { 'error', 'Internal error #992407: Incorrect number of readonly parameters' }
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
     *  Get IDs of transactions that are currently processing ("in play").
     *  These are transactions that are not queued, and not sleeping, and not complete.
     */
    connection.defineCommand("datp_transactionsInPlay", {
      numberOfKeys: 1,
      lua: `
      local processingKey = KEYS[1]
      local startIndex = tonumber(ARGV[1])
      local numRequired = tonumber(ARGV[2])

      -- numRequired = 30

      local list = { }
      local txIds = redis.call('zrangebyscore', '${KEYSPACE_PROCESSING}', 0, 9999999999999999, 'WITHSCORES', 'LIMIT', startIndex, numRequired)
      if 1 == 2 then
        return txIds
      end
      if txIds then
        -- txIds = [ txId1, sortOrder1, txId2, sortOrder2, ...]
        local num = #txIds / 2
        for i = 0, num-1 do
          -- Lua indexes start at 1
          local txId = txIds[1 + (i*2)]
          local score = txIds[2 + (i*2)]

          -- Add transaction states to our output
          local stateKey = '${KEYSPACE_STATE}' .. txId
          local txState = redis.call('hmget', stateKey,
            'owner',
            'externalId',
            'transactionType',
            'processingState',
            'startTime',
            'eventType',
            'queue',
            'pipeline',
            'nodeGroup',
            'lastUpdated',
            'ts')
          if txState then
            local owner = txState[1]
            local externalId = txState[2]
            local transactionType = txState[3]
            local processingState = txState[4]
            local startTime = txState[5]
            local eventType = txState[6]
            local queue = txState[7]
            local pipeline = txState[8]
            local nodeGroup = txState[9]
            local lastUpdated = txState[10]
            local ts = txState[11]

            list[#list + 1] = {
              txId,
              score,
              stateKey,
              'STATE-FOUND',
              owner,
              externalId,
              transactionType,
              processingState,
              startTime,
              eventType,
              queue,
              pipeline,
              nodeGroup,
              lastUpdated,
              ts
            }
          else
            list[#list + 1] = {
              txId,
              score,
              stateKey,
              'STATE-MISSING'
            }
          end
        end
      end

      -- Get state information for each transaction
      -- ZZZZZ
      return list
    `}) //- datp_transactionsInPlay

    /*
     *  Get IDs of transactions that are currently processing ("in play").
     *  These are transactions that are not queued, and not sleeping, and not complete.
     */
    connection.defineCommand("datp_transactionsSleeping", {
      numberOfKeys: 1,
      lua: `
      local processingKey = KEYS[1]
      local startIndex = tonumber(ARGV[1])
      local numRequired = tonumber(ARGV[2])

      -- numRequired = 30

      local list = { }
      local txIds = redis.call('zrangebyscore', '${KEYSPACE_SLEEPING}', 0, 9999999999999999, 'WITHSCORES', 'LIMIT', startIndex, numRequired)
      if 1 == 2 then
        return txIds
      end
      if txIds then
        -- txIds = [ txId1, sortOrder1, txId2, sortOrder2, ...]
        local num = #txIds / 2
        for i = 0, num-1 do
          -- Lua indexes start at 1
          local txId = txIds[1 + (i*2)]
          local score = txIds[2 + (i*2)]

          -- Add transaction states to our output
          local stateKey = '${KEYSPACE_STATE}' .. txId
          local txState = redis.call('hmget', stateKey,
            'owner',
            'externalId',
            'transactionType',
            'processingState',
            'startTime',
            'eventType',
            'queue',
            'pipeline',
            'nodeGroup',
            'lastUpdated',
            'ts')
          if txState then
            local owner = txState[1]
            local externalId = txState[2]
            local transactionType = txState[3]
            local processingState = txState[4]
            local startTime = txState[5]
            local eventType = txState[6]
            local queue = txState[7]
            local pipeline = txState[8]
            local nodeGroup = txState[9]
            local lastUpdated = txState[10]
            local ts = txState[11]

            list[#list + 1] = {
              txId,
              score,
              stateKey,
              'STATE-FOUND',
              owner,
              externalId,
              transactionType,
              processingState,
              startTime,
              eventType,
              queue,
              pipeline,
              nodeGroup,
              lastUpdated,
              ts
            }
          else
            list[#list + 1] = {
              txId,
              score,
              stateKey,
              'STATE-MISSING'
            }
          end
        end
      end

      -- Get state information for each transaction
      -- ZZZZZ
      return list
    `}) //- datp_transactionsSleeping

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
    connection.defineCommand("datp_transactionsToArchive", {
      numberOfKeys: 0,
      lua: `
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
          redis.call('zrem', '${KEYSPACE_TOARCHIVE}', txId)
        end

        -- Perhaps we aren't after any more to archive...
        if numRequired < 1 then
          return { }
        end

        -- Check this node is the one persisting transactions.
        local permittedNode = redis.call('get', '${KEYSPACE_TOARCHIVE_LOCK1}')
        if not permittedNode then

          -- No node currently is given permission to do persisting.
          if redis.call('get', '${KEYSPACE_TOARCHIVE_LOCK2}') then
            -- Do nothing until lock2 has expired.
            return { }
          end

          -- Okay, give this node permission
          --redis.call('set', '${KEYSPACE_TOARCHIVE_LOCK1}', nodeId)
          --redis.call('expire', '${KEYSPACE_TOARCHIVE_LOCK1}', ${PERSISTING_PERMISSION_LOCK1_PERIOD})
          redis.call('set', '${KEYSPACE_TOARCHIVE_LOCK1}', nodeId, 'EX', ${PERSISTING_PERMISSION_LOCK1_PERIOD})
          --redis.call('set', '${KEYSPACE_TOARCHIVE_LOCK2}', nodeId)
          --redis.call('expire', '${KEYSPACE_TOARCHIVE_LOCK2}', ${PERSISTING_PERMISSION_LOCK2_PERIOD})
          redis.call('set', '${KEYSPACE_TOARCHIVE_LOCK2}', nodeId, 'EX', ${PERSISTING_PERMISSION_LOCK2_PERIOD})
        elseif permittedNode ~= nodeId then

          -- Some other node is doing persisting
          return { }
        else

          -- This node already has permission to do persisting
        end

        -- What is the current time?
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis

        -- Get a list of transactions needing to be persisted.
        -- The score is the time after which to archive.
        local txIds = redis.call('zrange', '${KEYSPACE_TOARCHIVE}', 0, now, 'BYSCORE', 'LIMIT', 0, numRequired)
        local list = { }
        if txIds then
          -- Add transaction states to our output
          -- txIds = [ txId1, txId2, ...]
          for i = 1, #txIds do
            local txId = txIds[i]
            local stateKey = '${KEYSPACE_STATE}' .. txId
            local txState = redis.call('hmget', stateKey, 'processingState', 'event', 'state', 'pipeline', 'eventType')
            local processingState = txState[1]
            local event = txState[2]
            local state = txState[3]
            local pipeline = txState[4]
            local eventType = txState[5]

            if not processingState then
              -- Event is in list to be persisted, but we don't have a processingState/state!
              local msg = 'Internal Error: Transaction in archiving queue has no state saved [' .. txId .. ']'
              list[#list + 1] = { 'error', 'E37726', txId, msg }
            else
              list[#list + 1] = { 'transaction', txId, state }
            end
          end
        end
        --list[#list + 1] = now
        --return list
        --return txIds
        return list
    `}) //- datp_transactionsToArchive

}//- registerLuaScripts_transactions



export async function luaFindTransactions(filter, statusList) {
  // console.log(`luaFindTransactions(filter=${filter}, statusList=${statusList})`)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_findTransactions(filter, statusList)
    // console.log(`datp_findTransactions(): result=`, result)
    // console.log(`datp_findTransactions(): ${result.length} rows`)
    // result = [ ]
    'txId',
    'owner',
    'externalId',
    'transactionType',
    'processingState',
    'retry_wakeTime',
    'retry_wakeSwitch',
    'startTime',
    'lastUpdated'

    const txList = [ ]
    for (const row of result) {
      const txId = row[0]
      const owner = row[1]
      const externalId = row[2]
      const transactionType = row[3]
      const processingState = row[4]
      const wakeTime = row[5]
      const wakeSwitch = row[6]
      const startTime = row[7]
      const lastUpdated = parseInt(row[8])
      txList.push({
        txId,
        owner,
        externalId,
        transactionType,
        processingState,
        wakeTime,
        wakeSwitch,
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
 * Get a list of transactions currently 'in play'.
 * 
 * @param {Integer} startIndex Skip the transactions up to this position
 * @param {Integer} numRequired Number of transaction IDs to persist
 * @returns Array of transaction details
 */
export async function luaTransactionsInPlay(startIndex, numRequired) {
  // console.log(`luaTransactionsInPlay(${startIndex}, ${numRequired })`)

  await RedisLua._checkLoaded()
  const processingKey = `${KEYSPACE_TOARCHIVE}`
  // const stateKey = `${KEYSPACE_STATE}`
  // const persistLockKey2 = `${KEYSPACE_TOARCHIVE_LOCK2}`
  try {
    const connection = await RedisLua.regularConnection()
    const result = await connection.datp_transactionsInPlay(processingKey, startIndex, numRequired)
    // if (result.length > 0) {
      // console.log(`transactionsInPlay result=`, result)
    // }
    // console.log(`First of ${result.length}: `, result[0])

    const list = [ ]
    for (const rec of result) {
      list.push({
        /*
          txId,
          score,
          stateKey,
          'STATE-FOUND',
          owner,
          externalId,
          transactionType,
          status,
          startTime,
          eventType,
          queue,
          pipeline,
          nodeGroup,
          lastUpdated,
          ts
        */
        txId: rec[0],
        startTime: rec[1], // Step start time
        owner: rec[4],
        processingState: rec[7],
      })
    }
    return list
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_transactionsInPlay]`)
    throw e
  }
}//- transactionsInPlay


/**
 * Get a list of transactions currently 'in play'.
 * 
 * @param {Integer} startIndex Skip the transactions up to this position
 * @param {Integer} numRequired Number of transaction IDs to persist
 * @returns Array of transaction details
 */
export async function luaTransactionsSleeping(startIndex, numRequired) {
  // console.log(`luaTransactionsSleeping(${startIndex}, ${numRequired })`)

  await RedisLua._checkLoaded()
  const processingKey = `${KEYSPACE_TOARCHIVE}`
  // const stateKey = `${KEYSPACE_STATE}`
  // const persistLockKey2 = `${KEYSPACE_TOARCHIVE_LOCK2}`
  try {
    const connection = await RedisLua.regularConnection()
    const result = await connection.datp_transactionsSleeping(processingKey, startIndex, numRequired)
    // if (result.length > 0) {
      // console.log(`transactionsInPlay result=`, result)
    // }
    // console.log(`First of ${result.length}: `, result[0])

    const list = [ ]
    for (const rec of result) {
      list.push({
        /*
          txId,
          score,
          stateKey,
          'STATE-FOUND',
          owner,
          externalId,
          transactionType,
          status,
          startTime,
          eventType,
          queue,
          pipeline,
          nodeGroup,
          lastUpdated,
          ts
        */
        txId: rec[0],
        startTime: rec[1], // Step start time
        owner: rec[4],
        processingState: rec[7],
      })
    }
    return list
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [transactionsSleeping]`)
    throw e
  }
}//- transactionsSleeping



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
export async function luaTransactionsToArchive(persistedTransactionIds, nodeId, numRequired) {
  // console.log(`----- luaTransactionsToArchive(persistedTransactionIds, ${nodeId}, ${numRequired})`, persistedTransactionIds)
  await RedisLua._checkLoaded()
  try {
    const connection = await RedisLua.regularConnection()
    const result = await connection.datp_transactionsToArchive(nodeId, numRequired, persistedTransactionIds.length, persistedTransactionIds)
    // if (result.length > 0) {
    //   console.log(`datp_transactionsToArchive result=`.magenta, result)
    //   console.log(`datp_transactionsToArchive result=`.magenta, result.length)
    // }
    return result
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_transactionsToArchive]`)
    throw e
  }
}

