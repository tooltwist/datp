/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { FLOW_VERBOSE, KEYSPACE_EXCEPTION, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_IN, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_WAKEUP_PROCESSING_LOCK, MIN_WAKEUP_PROCESSING_INTERVAL, PROCESSING_STATE_QUEUED, RedisLua } from './redis-lua';

export async function registerLuaScripts_sleep() {
  // console.log(`registerLuaScripts_sleep() `.magenta)
  const connection = await RedisLua.regularConnection()

  /*
    *  Take a transaction out of sleep mode and put it in a queue to be processed.
    */
  connection.defineCommand("datp_manuallyWake", {
    numberOfKeys: 1,
    lua: `
    local stateKey = KEYS[1]
    local txId = ARGV[1]
    local log = { }

    -- See if the transaction is sleeping
    local score = redis.call('zscore', '${KEYSPACE_SLEEPING}', txId)
    if not score then
      -- Is not in sleeping list
      return { 'error', 'Transaction not in sleeping list' }
    end

    -- Get the current time
    --local nowArr = redis.call('TIME')
    --local seconds = nowArr[1] + 0
    --local millis = math.floor(nowArr[2] / 1000)
    --local now = (seconds * 1000) + millis

    -- Get the retry details
    local stateFields = redis.call('HMGET', stateKey,
      'processingState',
      'event_nodeGroup')
    local processingState = stateFields[1]
    local nodeGroup = stateFields[2]

    if processingState ~= 'sleeping' then
      return { 'error', { 'The processingState is not \"sleeping\" [' .. processingState .. ']' } }
    end
    
    -- remove from the sleeping list
    -- (and the processing list, for good measure - it should not be there)
    redis.call('ZREM', '${KEYSPACE_SLEEPING}', txId)
    redis.call('ZREM', '${KEYSPACE_PROCESSING}', txId)
    log[#log+1] = 'Removed from the sleeping list'
    log[#log+1] = 'Removed from the processing list'

    -- set the processing state
    -- clear the retry fields and any switch
    redis.call('HMSET', stateKey,
      'processingState', '${PROCESSING_STATE_QUEUED}',
      'retry_sleepingSince', 0,
      'retry_wakeTime', 0,
      'retry_wakeSwitch', '')
    log[#log+1] = 'Set the processing state to ' .. '${PROCESSING_STATE_QUEUED}'

    -- Add to the input queue for the nodeGroup
    local queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
    log[#log+1] = 'Added the transaction to queue [' .. queueKey .. ']'
    redis.call('rpush', queueKey, txId)

    return { 'ok', log }
  `}) //- datp_manuallyWake


  /*
    *  For each of the transactions in the sleeping list where it's wake time has passed:
    *  - remove from the sleeping list, and the proccesing list (should not be there)
    *  - chack the status is 'sleeping'. If not, create an exception for the transaction.
    *  - clear the retry fields
    *  - add the transaction to the appropriate queue
    */
  connection.defineCommand("datp_wakeupProcessing", {
    numberOfKeys: 0,
    lua: `
    local exceptionTxIds = { }

    -- Prevent excessive usage from multiple nodes calling this LUA script
    -- https://redis.io/commands/setnx/
    -- https://redis.io/commands/expire/
    local lockAdded = redis.call('SETNX', '${KEYSPACE_WAKEUP_PROCESSING_LOCK}', "XXX")
    if lockAdded == 0 then
      return { 'notyet', 'Wait a while before trying again', lockAdded }
    end
    local ttl1 = redis.call('TTL', '${KEYSPACE_WAKEUP_PROCESSING_LOCK}')
    redis.call('EXPIRE', '${KEYSPACE_WAKEUP_PROCESSING_LOCK}', ${MIN_WAKEUP_PROCESSING_INTERVAL})
    local ttl2 = redis.call('TTL', '${KEYSPACE_WAKEUP_PROCESSING_LOCK}')

    -- Get the current time
    local nowArr = redis.call('TIME')
    local seconds = nowArr[1] + 0
    local millis = math.floor(nowArr[2] / 1000)
    local now = (seconds * 1000) + millis

    -- Get a list of all transactions that have reached their wakeTime
    local startIndex = 0
    local numRequired = 100
    local txIds = redis.call('ZRANGEBYSCORE', '${KEYSPACE_SLEEPING}', 0, now, 'LIMIT', startIndex, numRequired)

    for i = 1, #txIds do
      -- Lua indexes start at 1
      local txId = txIds[i]
      local stateKey = '${KEYSPACE_STATE}' .. txId

      -- remove from the sleeping list
      -- (and the processing list, for good measure - it should not be there)
      redis.call('ZREM', '${KEYSPACE_SLEEPING}', txId)
      redis.call('ZREM', '${KEYSPACE_PROCESSING}', txId)

      -- Get the retry details
      local stateFields = redis.call('HMGET', stateKey,
        'processingState',
        'event_nodeGroup')
      local processingState = stateFields[1]
      local nodeGroup = stateFields[2]

      if processingState ~= 'sleeping' then

        -- exception, not sleeping
        redis.call('HMSET', stateKey,
          'exception_reason', 'In sleeping list but status is not sleeping',
          'exception_from', 'datp_wakeupProcessing',
          'exception_time', now)
          redis.call('ZADD', '${KEYSPACE_EXCEPTION}', now, txId)
          exceptionTxIds[#exceptionTxIds+1] = txId

      else

        -- is sleeping
        -- set the processing state
        -- clear the retry fields and any switch
        redis.call('HMSET', stateKey,
          'processingState', '${PROCESSING_STATE_QUEUED}',
          'retry_sleepingSince', 0,
          'retry_wakeTime', 0,
          'retry_wakeSwitch', '')

        -- Add to the input queue for the nodeGroup
        local queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
        redis.call('rpush', queueKey, txId)
      end
    end -- next txId
    return { 'ok', txIds, exceptionTxIds }
  `}) //- datp_wakeupProcessing


  /*
   *  Take a transaction out of sleep mode and put it in a queue to be processed.
   */
  connection.defineCommand("datp_setSwitch", {
    numberOfKeys: 1,
    lua: `
    local stateKey = KEYS[1]
    local txId = ARGV[1]
    local newSwitch = ARGV[2]
    local newValue = ARGV[3]

    -- Get the current time
    --local nowArr = redis.call('TIME')
    --local seconds = nowArr[1] + 0
    --local millis = math.floor(nowArr[2] / 1000)
    --local now = (seconds * 1000) + millis

    -- Get the transaction details
    local stateFields = redis.call('HMGET', stateKey,
      'processingState',
      'event_nodeGroup',
      'switches',
      'retry_wakeSwitch')
    local processingState = stateFields[1]
    local nodeGroup = stateFields[2]
    local switches = stateFields[3]
    local wakeSwitch = stateFields[4]
    if not switches then
      switches = ''
    end


    -- trim spaces and any commas
    -- http://lua-users.org/wiki/StringTrim
    -- https://www.educba.com/lua-regex/
    newSwitch = newSwitch:match("^%s*([^%s,]*).-$")
    newValue = newValue:match("^%s*([^%s,]*).-$")


    -- Split the existing switches by comma (e.g. aaa=123,b,c=345)
    -- https://stackoverflow.com/questions/19262761/lua-need-to-split-at-comma
    local foundSwitch = 0
    local changedValue = 0
    local needToWake = 0
    local names = { } -- may include ! to indicated "unacknowledged"
    local values = { }
    for word in string.gmatch(switches, '([^,]+)') do

      -- The definition will be either a switch name (xyz), or a name value pair (xyz=123)
      local name, value = word:match('^([^=]*)=(.-)$')
      if not name then
        name = word
        value = ''
      end

      -- Can be either !xyz to xyz, depending on whether this is an unacknowledged value
      -- See README-switches.md for details
      local actualName = name:match('!*(.-)$')

      -- See if this is the same as the switch being set
      if actualName ~= newSwitch then

        -- some other switch: keep the existing value
        names[#names+1] = name
        values[#values+1] = value
      else

        -- The same switch we are setting: see if we are deleting it
        if newValue == '-' then

          -- we are deleting this switch - consider the value changed
          changedValue = 1

        elseif value == newValue then

          -- The switch we are setting already has the required value
          foundSwitch = 1
          names[#names+1] = name
          values[#values+1] = newValue

        else
          -- The switch we are setting is changing value
          foundSwitch = 1
          names[#names+1] = '!' .. actualName
          values[#values+1] = newValue
          changedValue = 1
        end

        -- Are we triggering a wake?
        if processingState == 'sleeping' and newSwitch == wakeSwitch and changedValue == 1 then
          needToWake = 1
        end
      end
    end

    -- If the new switch was not in the list, add it
    if foundSwitch == 0 and newValue ~= '-' then
      changedValue = 1
      names[#names+1] = '!' .. newSwitch
      values[#values+1] = newValue
      -- Are we triggering a wake?
      if processingState == 'sleeping' and newSwitch == wakeSwitch then
        needToWake = 1
      end
  end

    -- Construct the new switches definition
    local newSwitches = ''
    local sep = ''
    for i = 1, #names do
      local name = names[i]
      local value = values[i]
      if value == '' then
        newSwitches = newSwitches .. sep .. name
      else
        newSwitches = newSwitches .. sep .. name .. '=' .. value
      end
      sep =','
    end -- for

    -- If nothing has changed, simply return
    if newSwitches == switches then
      return { 'nochange', newSwitches, newSwitch, newValue, names, values }
    end

    -- If we are not waiting on this switch, just update REDIS and return
    if needToWake == 0 then
      redis.call('HSET', stateKey, 'switches', newSwitches)
      return { 'updated', newSwitches, wakeSwitch, newSwitch, newValue, processingState, changedValue }
    end

    
    -- We ARE waiting on this switch, and it has changed.
    -- remove from the sleeping list
    -- (and the processing list, for good measure - it should not be there)
    redis.call('ZREM', '${KEYSPACE_SLEEPING}', txId)
    redis.call('ZREM', '${KEYSPACE_PROCESSING}', txId)

    -- set the processing state
    -- clear the retry fields and any switch
    redis.call('HMSET', stateKey,
      'processingState', '${PROCESSING_STATE_QUEUED}',
      'retry_sleepingSince', 0,
      'retry_wakeTime', 0,
      'retry_wakeSwitch', '',
      'switches', newSwitches)

    -- Add to the input queue for the nodeGroup
    local queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
    redis.call('rpush', queueKey, txId)

    return { 'woken', newSwitches }
  `}) //- datp_setSwitch


  /*
   *  Get a current switch value from REDIS.
   */
  connection.defineCommand("datp_getSwitch", {
    numberOfKeys: 1,
    lua: `
    local stateKey = KEYS[1]
    local txId = ARGV[1]
    local requiredSwitch = ARGV[2]
    local acknowledgeValue = tonumber(ARGV[3])

    -- Get the transaction details from REDIS
    local stateFields = redis.call('HMGET', stateKey,
      'processingState',
      'switches')
    local processingState = stateFields[1]
    local switches = stateFields[2]
    if not switches then
      switches = ''
    end

    -- Get the switch value
    local foundTheSwitchValue = 0
    local switchValue = ''

    -- Split the existing switches by comma (e.g. aaa=123,b,!c=345)
    -- https://stackoverflow.com/questions/19262761/lua-need-to-split-at-comma
    local names = { } -- may include ! to indicated "unacknowledged"
    local values = { }
    for word in string.gmatch(switches, '([^,]+)') do

      -- The definition will be either a switch name (xyz), or a name value pair (xyz=123)
      -- Switch names prefixed with ! indicate an "unacknowledged value".
      -- (See README-switches.md for details)
      local name, value = word:match('^([^=]*)=(.-)$')
      if not name then
        name = word
        value = ''
      end

      -- Let's get the actual name, without any ! prefix.
      local actualName = name:match('!*(.-)$')

      -- Find the value of the required switch, and adjust the switches if necessary.
      -- If this is called from a step asking for the switch value, then acknowledgeValue
      -- will be true, and we need remove any ! from the front of the requested switch's name.
      if actualName == requiredSwitch then
        -- this is it
        foundTheSwitchValue = 1
        switchValue = value
        if acknowledgeValue == 1 then
          -- remove any "unacknowledged flag" from the name (the ! prefix)
          names[#names+1] = actualName
          values[#values+1] = value
        else
          names[#names+1] = name
          values[#values+1] = value
        end
      else
        names[#names+1] = name
        values[#values+1] = value
      end
    end

    -- Construct the new switches definition
    local newSwitches = ''
    local sep = ''
    for i = 1, #names do
      local name = names[i]
      local value = values[i]
      if value == '' then
        newSwitches = newSwitches .. sep .. name
      else
        newSwitches = newSwitches .. sep .. name .. '=' .. value
      end
      sep =','
    end -- for
    if newSwitches ~= switches then
      redis.call('HSET', stateKey, 'switches', newSwitches)
    end

    -- Return the switch value and other info.
    -- We need boolean 'foundTheSwitchValue', because LUA cannot return a nil value for switchValue.
    return { 'ok', foundTheSwitchValue, switchValue, switches, newSwitches, processingState }
  `}) //- datp_getSwitch

}//- registerLuaScripts_sleep


/**
 * Put a transaction to sleep, waiting for either a prescribed amount of time, 
 * of for a switch to be set.
 * 
 * @param {*} txId 
 * @param {*} nameOfSwitch 
 * @param {*} sleepDuration 
 */
export async function luaManuallyWake(txId) {
  if (FLOW_VERBOSE) console.log(`----- luaManuallyWake(txId=${txId})`.yellow)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    result = await connection.datp_manuallyWake(
      // Keys
      stateKey,
      // Parameters
      txId,
    )
    // console.log(`datp_manuallyWake returned`.red, result)

    const woken = (result[0] != 0)
    const log = result[1]
    // console.log(`woken=`, woken)
    return {
      woken,
      log
    }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_manuallyWake]`)
    throw e
  }
}//- luaManuallyWake



/**
 *    Lua will look in the sleeping list for transactions who's wakeup time has passed
 *    and move them over to a processing queue (after setting status's, etc).
 */
export async function luaWakeupProcessing() {
  if (FLOW_VERBOSE) console.log(`----- luaWakeupProcessing()`.yellow)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_wakeupProcessing() // { code, txIds[], exeptionTxIds[] }
    switch (result[0]) {
      case 'notyet':
        break

      case 'ok':
        if (result[1].length > 0) {
          console.log(`${result[1].length} transactions woken from sleep`)
        }
        if (result[2].length > 0) {
          console.log(`${result[2].length} transactions had exceptions while waking from sleep`)
        }
        break

      default:
        console.log(`Unknown result from datp_wakeupProcessing:`, result)
    }

  } catch (e) {
    console.log(`Exception calling LUA script [datp_wakeupProcessing]`, e)
  }
  return { code: 'unknown' }
}//- luaWakeupProcessing


/**
 *    Set a switch for a transaction, and if it is waiting on the switch, move it over to a processing queue (after setting status's, etc).
 */
export async function luaSetSwitch(txId, switchName, value) {
  if (FLOW_VERBOSE) console.log(`----- luaSetSwitch(txId=${txId}, switchName=${switchName}, value=${value})`.yellow)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    result = await connection.datp_setSwitch(
      // Keys
      stateKey,
      // Parameters
      txId,
      switchName,
      value
    )
    // console.log(`datp_setSwitch returned`.red, result)

    const action = result[0]
    const newSwitches = result[1]
    // console.log(`action=`, action)
    // console.log(`newSwitches=`, newSwitches)
    return {
      action,
      newSwitches
    }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_setSwitch]`)
    throw e
  }
}//- luaSetSwitch


/**
 *    Set a switch for a transaction, and if it is waiting on the switch, move it over to a processing queue (after setting status's, etc).
 */
export async function luaGetSwitch(txId, switchName, acknowledgeValue=false) {
  if (FLOW_VERBOSE) console.log(`----- luaGetSwitch(txId=${txId}, switchName=${switchName}, acknowledgeValue=${acknowledgeValue})`.yellow)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    result = await connection.datp_getSwitch(
      // Keys
      stateKey,
      txId,
      switchName,
      acknowledgeValue ? 1 : 0
    )
    // console.log(`datp_getSwitch returned`.red, result)

    const action = result[0]
    const found = result[1]
    const value = result[2]
    const oldSwitches = result[3]
    const newSwitches = result[4]
    const processingState = result[5]

    return {
      value: found ? value : null,
      oldSwitches,
      newSwitches,
      processingState,
    }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_getSwitch]`)
    throw e
  }
}//- luaGetSwitch
