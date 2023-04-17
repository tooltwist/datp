/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { KEYSPACE_EXCEPTION, KEYSPACE_PROCESSING, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_TOARCHIVE, RedisLua } from './redis-lua';

export async function registerLuaScripts_exceptions() {
  // console.log(`registerLuaScripts_exceptions() `.magenta)
  const connection = await RedisLua.regularConnection()



    /*
     *  Get IDs of transactions in the 'exception' list.
     */
    connection.defineCommand("datp_exceptionTransactions", {
      numberOfKeys: 1,
      lua: `
      local processingKey = KEYS[1]
      local startIndex = tonumber(ARGV[1])
      local numRequired = tonumber(ARGV[2])

      -- numRequired = 30

      local list = { }
      local txIds = redis.call('zrangebyscore', '${KEYSPACE_EXCEPTION}', 0, 9999999999999999, 'WITHSCORES', 'LIMIT', startIndex, numRequired)
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
            'ts',
            'exception_reason',
            'exception_from',
            'exception_time')
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
            local exceptionReason = txState[12]
            local exceptionFrom = txState[13]
            local exceptionTime = txState[14]

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
              ts,
              exceptionReason,
              exceptionFrom,
              exceptionTime
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
      return list
    `}) //- datp_exceptionTransactions

    /*
     *  Put a transaction in the orphan list - there is something wrong with it.
     */
    connection.defineCommand("datp_markAsException", {
      numberOfKeys: 1,
      lua: `
      local stateKey = KEYS[1]
      local txId = ARGV[1]
      local exceptionReason = ARGV[2]
      local exceptionFrom = ARGV[3]
      
      -- Get the current time
      local nowArr = redis.call('TIME')
      local seconds = nowArr[1] + 0
      local millis = math.floor(nowArr[2] / 1000)
      local now = (seconds * 1000) + millis

      -- remove from the sleeping list and the processing list
      redis.call('ZREM', '${KEYSPACE_SLEEPING}', txId)
      redis.call('ZREM', '${KEYSPACE_PROCESSING}', txId)

      -- remember the exception exceptionReason
      redis.call('HMSET', stateKey,
        'exception_reason', exceptionReason,
        'exception_from', exceptionFrom,
        'exception_time', now)

      -- add to the orphan list
      redis.call('ZADD', '${KEYSPACE_EXCEPTION}', now, txId)
    `}) //- datp_markAsException

}

/**
 * Put a transaction into the orphan list, as it is in an unknown state
 * 
 * @param {String} txId 
 * @param {String} reason
 */
export async function luaMarkAsException(txId, reason, from) {
  if (FLOW_VERBOSE) console.log(`----- luaMarkAsException(txId=${txId}, reason=${reason}, from="${from})`.yellow)
  let result
  try {
    const connection = await RedisLua.regularConnection()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    result = await connection.datp_markAsException(
      // Keys
      stateKey,
      // Parameters
      txId,
      reason,
      from
    )
    console.log(`luaMarkAsException returned`.red, result)
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_markAsException]`)
    throw e
  }
}//- luaMarkAsException


/**
 * Get a list of transactions currently 'in play'.
 * 
 * @param {Integer} startIndex Skip the transactions up to this position
 * @param {Integer} numRequired Number of transaction IDs to persist
 * @returns Array of transaction details
 */
export async function luaTransactionsWithException(startIndex, numRequired) {
  // console.log(`luaTransactionsWithException(${startIndex}, ${numRequired })`)

  await RedisLua._checkLoaded()
  const processingKey = `${KEYSPACE_TOARCHIVE}`
  // const stateKey = `${KEYSPACE_STATE}`
  // const persistLockKey2 = `${KEYSPACE_TOARCHIVE_LOCK2}`
  try {
    const connection = await RedisLua.regularConnection()
    const result = await connection.datp_exceptionTransactions(processingKey, startIndex, numRequired)
    // if (result.length > 0) {
      // console.log(`transactionsBroken result=`, result)
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
        startTime: parseInt(rec[1]), // Step start time
        owner: rec[4],
        processingState: rec[7],
        exceptionReason: rec[15],
        exceptionFrom: rec[16],
        exceptionTime: parseInt(rec[17])
      })
    }
    return list
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [luaTransactionsWithException]`)
    throw e
  }
}//- luaTransactionsWithException

