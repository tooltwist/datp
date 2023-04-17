/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert';
import { schedulerForThisNode } from '../../..';
import { DEFINITION_PROCESS_STEP_START_EVENT, EVENT_DEFINITION_STEP_START_SCHEDULED, STEP_DEFINITION, validateStandardObject } from '../eventValidation';
import Scheduler2 from '../Scheduler2';
import Transaction from '../TransactionState';
import { de_externalizeTransactionState, EXTERNALID_UNIQUENESS_PERIOD, externalizeTransactionState, FLOW_PARANOID, FLOW_VERBOSE, KEYSPACE_EXTERNALID, KEYSPACE_PIPELINE, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, NUMBER_OF_EXTERNALIZED_FIELDS, PROCESSING_STATE_QUEUED, PROCESSING_STATE_SLEEPING, RedisLua, SHOWLOG, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3 } from './redis-lua';

export async function registerLuaScripts_startStep() {
  // console.log(`registerLuaScripts_startStep() `.magenta)
  const connection = await RedisLua.regularConnection()



  /*
    *  Add an event to a queue.
    *  1. If externalId is provided, check it is not already used.
    *  2. Check this transaction does not already have queued status.
    *  3. If pipeline is provided, get pipeline definition and nodeGroup.
    * 3a. If mode is 'start-transaction', check the pipeline can be used s a transaction.
    *  4. Determine the queue key.
    *  5. Save the transaction state.
    *  6. Save the event.
    *  7. Remove it from the 'processing' list
    *  8. Update statistics
    * 
    * Operation will be one of:
    *    start-transaction - start a new transaction (check externalId)
    *    start-pipeline - start a pipeline ('pipeline' provides the pipeline and nodeGroup)
    *    retry-step - prepare for step retry and set 'wakeTime' in sleeping list
    */
  // See Learn Lua in 15 minutes (https://tylerneylon.com/a/learn-lua/)
  connection.defineCommand("datp_startStep", {
    numberOfKeys: 1,
    lua: `
      local stateKey = KEYS[1]

      -- arguments to this function
      local mode = ARGV[1]
      local txId = ARGV[2]
      local owner = ARGV[3]
      local externalId = ARGV[4] -- check for uniqueness if mode=="start-transaction"
      local pipeline = ARGV[5]
      local nodeGroup = ARGV[6]
      local stateJSON = ARGV[7]
      local eventJSON = ARGV[8]
      local eventF2i = ARGV[9]
      local delayBeforeQueueing = ARGV[10] -- used if mode=="retry-step"
      local wakeSwitch = ARGV[11] -- optional if mode=="retry-step"

      -- variables
      local INDEX_OF_EXTERNALIZED_FIELDS = 12

      local log = { }
      local pipelineKey = '${KEYSPACE_PIPELINE}' .. pipeline
      log[#log+1] = 'txId: ' .. txId
      log[#log+1] = 'owner: ' .. owner
      log[#log+1] = 'externalId: ' .. externalId
      log[#log+1] = 'pipeline: ' .. pipeline
      log[#log+1] = 'nodeGroup: ' .. nodeGroup
      log[#log+1] = 'delayBeforeQueueing: ' .. delayBeforeQueueing
      log[#log+1] = 'wakeSwitch: ' .. wakeSwitch

      -- 1. If externalId is provided
      -- 1a. Check it is not already used
      -- 1b. Remember the externalId
      if mode == 'start-transaction' and externalId ~= '' then
        -- Check it is not already set
        local externalIdKey = '${KEYSPACE_EXTERNALID}' .. owner .. ':::' .. externalId
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
        local msg = 'Internal Error: datp_startStep: Transaction is already queued'
        return { 'error', 'E99288', txId, msg }
      end
      log[#log+1] = 'Checked transaction does not already have queued status'

      -- 3. If a pipeline is specified, get the nodeGroup from the pipeline definition.
      local pipelineVersion = ''
      if pipeline ~= '' and pipeline ~= nil then  
        local details = redis.call('HMGET', pipelineKey,
          'version',
          'nodeGroup',
          'isTransactionType')
        pipelineVersion = details[1]
        nodeGroup = details[2]
        local isTransactionType = details[3]

        if not pipelineVersion then
          local msg = 'Internal Error: datp_startStep: pipeline definition not loaded [' .. pipeline .. ']'
          return { 'error', 'E911662', pipeline, msg }
        end
        log[#log+1] = 'Got pipeline definition (isTransactionType=' .. isTransactionType .. ')'
        log[#log+1] = 'Using pipeline nodeGroup [' .. nodeGroup .. ']'

        -- If this pipeline is being used as a transaction type, check that us allowed
        if mode == 'start-transaction' and isTransactionType ~= '1' then
          local msg = 'Internal Error: datp_startStep: pipeline does not permit use as a transaction [' .. pipeline .. ']'
          return { 'error', 'E937662', pipeline, msg }
        end
      end
      
      -- 4. Determine the queue key
      local queueKey= '${KEYSPACE_QUEUE_IN}' .. nodeGroup
      log[#log+1] = 'Using queue ' .. queueKey

      -- Get the current time
      local nowArr = redis.call('TIME')
      local seconds = nowArr[1] + 0
      local millis = math.floor(nowArr[2] / 1000)
      local now = (seconds * 1000) + millis
      log[#log+1] = 'Current time is ' .. now

      -- 5. Save the transaction state
      redis.call('hset', stateKey,
        'txId', txId,
        'owner', owner,
        'externalId', externalId,
        'state', stateJSON,
        'event', eventJSON,
        'event_f2i', eventF2i,
        'event_pipeline', pipeline,
        'event_nodeGroup', nodeGroup,
        'event_type', '${Scheduler2.STEP_START_EVENT}',
        'eventType', '${Scheduler2.STEP_START_EVENT}',
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
        return { 'error', 'E9626322', 'datp_startStep', 'Incorrect number of parameters' }
      end
      log[#log+1] = 'Saved the transaction state as JSON and externalized fields'

      -- 6. Add to either the queue, or the sleeping list
      --if eventJSON ~= '' then
      --  redis.call('rpush', queueKey, txId)
      --  log[#log+1] = 'Added this transaction to the queue'
      --end

      local whenToEnqueue = 0
      local queueImmediately = 0 -- If set, we don't wait for a switch, because it's already been updated.
      if mode=='start-transaction' or mode=='start-pipeline' then

        --
        -- queued to continue processing
        --
        log[#log+1] = 'Added this transaction to the queue ' .. queueKey
        redis.call('rpush', queueKey, txId)
        redis.call('HSET', stateKey,
          'processingState', '${PROCESSING_STATE_QUEUED}',
          -- not sleeping
          'retry_sleepingSince', 0,
          'retry_counter', 0,
          'zzzzzz_counterReset_1', 123,
          'retry_wakeTime', 0,
          'retry_wakeSwitch', '')
      elseif mode=='retry-step' then

        --
        --  RETRY - SLEEPING, OR WAIT FOR A SWITCH.
        --
        -- (Waiting for a switch also has a (long) sleep)
        --
        redis.call('HINCRBY', stateKey, 'retry_counter', 1)

        -- See if a specified wake switch has an unacknowledged update. That means that it's value was changed
        -- between when the step asked for the step value and now, by some external non-pipeline/step code.
        -- Unacknowledged transactions are indicated by a ! prefix to the switch name.
        -- See README-switches.md for more details.
        -- If the switch has the unacknowledge update flag set, we'll add the step to the queue immediately.
        -- If not, we have the state ready in REDIS, but wait for setSwitch to add it to the queue.
        if wakeSwitch then
          log[#log+1] = 'This step waits for switch "' .. wakeSwitch .. '"'

          -- Split the existing switches by comma (e.g. aaa=123,b,c=345)
          -- https://stackoverflow.com/questions/19262761/lua-need-to-split-at-comma
          local switches = redis.call('HGET', stateKey, 'switches')
          if not switches then
            switches = ''
          end
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
            names[#names+1] = actualName
            values[#values+1] = value

            -- See if this is the same as the switch we are waiting on
            if actualName == wakeSwitch and name ~= actualName then
                queueImmediately = 1
            end
          end -- for

          -- Construct the new switches definition, which now has the ! prefixes removed
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
            log[#log+1] = 'Changing switches from ' .. switches .. ' to ' .. newSwitches
            redis.call('HSET', stateKey, 'switches', newSwitches)
          else
            log[#log+1] = 'No changes to switches: ' .. newSwitches
          end
        end -- wakeSwitch

        -- If there was already an (unacknowledged) update to the switch we need to
        -- wait on, then we can push the transaction to the queue for processing. If not,
        -- then we add it to the sleeping list so the normal sleep can occur (but it might
        -- be woken if the switch we are waiting on has it's value changed).
        if queueImmediately == 1 then

          -- run immediately
          log[#log+1] = 'Switch had unacknowledged updates. We will queue immediately.'
          redis.call('HSET', stateKey,
            'processingState', '${PROCESSING_STATE_QUEUED}', -- bypass sleeping
            'retry_sleepingSince', 0,
            'retry_wakeTime', 0,
            'retry_wakeSwitch', '')
          redis.call('RPUSH', queueKey, txId)

        else

          -- go to sleep
          log[#log+1] = 'Transaction is going to sleep now...'
          whenToEnqueue = now + delayBeforeQueueing
          redis.call('ZADD', '${KEYSPACE_SLEEPING}', whenToEnqueue, txId)
          redis.call('hset', stateKey,
            'processingState', '${PROCESSING_STATE_SLEEPING}',
            'retry_sleepingSince', now,
            'retry_wakeTime', whenToEnqueue,
            'retry_wakeSwitch', wakeSwitch)

        end -- queueImmediately

      else

        --
        --  ERROR - NOT STARTING TRANSACTION, PIPELINE, OR ENTERING RETRY MODE
        --
        local msg = 'Internal Error: datp_startStep: invalid mode [' .. mode .. ']'
        return { 'error', 'E99288', txId, msg }
      end
      
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
      if mode == 'start-transaction' then
        redis.call('hincrby', statsKey1, f_txStart, 1)
      end
      if pipeline ~= '' then
        redis.call('hincrby', statsKey1, f_pipelineStart, 1)
        redis.call('hincrby', statsKey1, f_ownerPipeline, 1)
        if mode == 'start-transaction' then
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
      if mode == 'start-transaction' then
        redis.call('hincrby', statsKey2, f_txStart, 1)
      end
      if pipeline ~= '' then
        redis.call('hincrby', statsKey2, f_pipelineStart, 1)
        redis.call('hincrby', statsKey2, f_ownerPipeline, 1)
        if mode == 'start-transaction' then
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
      --redis.call('set', '@start', timeslot3)
      --redis.call('set', '@expire', expiry3)
      redis.call('hincrby', statsKey3, f_start, 1)
      redis.call('hincrby', statsKey3, f_ownerStart, 1)
      redis.call('hincrby', statsKey3, f_nodeGroup, 1)
      if mode == 'start-transaction' then
        redis.call('hincrby', statsKey3, f_txStart, 1)
      end
      if pipeline ~= '' then
        redis.call('hincrby', statsKey3, f_pipelineStart, 1)
        redis.call('hincrby', statsKey3, f_ownerPipeline, 1)
        if mode == 'start-transaction' then
          redis.call('hincrby', statsKey3, f_txPipelineStart, 1)
        end
      end
      redis.call('hset', statsKey3, f_qInLength, inputLength)
      redis.call('hset', statsKey3, f_qOutLength, outputLength)
      redis.call('expireat', statsKey3, expiry3)
      log[#log+1] = 'Updated the metrics collection'
      
      return { 'Zok', queueKey, nodeGroup, pipeline, pipelineVersion, whenToEnqueue, log }
    `
  });//- datp_startStep
}//- registerLuaScripts_startStep


/**
 * Add an event to a queue.
 * This calls LUA script datp_enqueue, that updates multiple REDIS tables.
 * 
 * If pipeline is specified, the pipeline definition is used to determine the nodegroup,
 * and the pipeline definition will be appended to the reply when the event is dequeued.
 * 
 * @param {string} mode start-transaction | start-pipeline | retry-step
 * @param {string} queueType in | out | admin
 * @param {string} pipeline If provided, we'll get the nodeGroup from the pipelne
 * @param {string} nodeGroup
 * @param {object} event
 */
export async function luaEnqueue_startStep(mode, txState, event, delayBeforeQueueing=0, wakeSwitch=null) {
  if (FLOW_VERBOSE) console.log(`----- luaEnqueue_startStep(mode=${mode}, txState, event, delayBeforeQueueing=${delayBeforeQueueing}ms, wakeSwitch=${wakeSwitch})`.brightRed)
  if (FLOW_VERBOSE) console.log(`----- event=${JSON.stringify(event, '', 2)}`.gray)

  /*
   *  The f2i in the event gives us access to the other required values.
   */
  assert(mode=='start-transaction' || mode=='start-pipeline' || mode=='retry-step')

  // Check the event is valid
  validateStandardObject('luaEnqueue_startStep() checking event', event, EVENT_DEFINITION_STEP_START_SCHEDULED)

  // We need either a pipeline name, or a node group, but not both.
  // If we have a pipelineName the LUA script will use it to get the nodeGroup
  // where that pipeline is run. If we are not starting a new pipeline, then
  // we run the step in the current node.
  const stepId2 = txState.getF2stepId(event.f2i)
  const S = txState.stepData(stepId2)
  let pipelineName
  let nodeGroup
  if (typeof(S.stepDefinition) === 'string') {
    pipelineName = S.stepDefinition
    nodeGroup = null
  } else {
    pipelineName = null
    nodeGroup = schedulerForThisNode.getNodeGroup()
  }

  assert(typeof(event) === 'object' && event != null)
  assert(event.eventType === Scheduler2.STEP_START_EVENT)
  assert (typeof(stepId2) === 'string')
  assert(S.fullSequence)
  assert(S.vogPath)
  assert(S.stepDefinition) //RRRR

  if (FLOW_PARANOID) {
    const s = txState.stepData(stepId2)
    validateStandardObject('luaEnqueue_startStep() checking step (paranoid)', s, STEP_DEFINITION)
    validateStandardObject('luaEnqueue_startStep() event', event, DEFINITION_PROCESS_STEP_START_EVENT)
    assert(txState instanceof Transaction)
    txState.vog_validateSteps()
  }


  const txId = txState.getTxId()
  const owner = txState.getOwner()
  const externalId = txState.getExternalId()
  const metadata = txState.vog_getMetadata()
  const webhook = (metadata.webhook) ? metadata.webhook : ''


// console.log(`event=`.magenta, event)
  const tmpTxState = event.txState
  delete event.txState //ZZZ Should not be set
  const eventJSON = JSON.stringify(event)
  event.txState = tmpTxState
// console.log(`eventJSON=`.magenta, eventJSON)

  // // Is this the initial transaction?
  // const txStateObject = txState.asObject()
  // // const isInitialTransaction = (txStateObject.f2.length <= 3)
  // // console.log(`isInitialTransaction=`.gray, isInitialTransaction)


  const stateKey = `${KEYSPACE_STATE}${txId}`

  // Extract the externalized fields from the transaction state so they can be used by LUA scripts.
  // If we delete them from the state after extraction, then the stateJSON is a little bit smaller, but it's probably not worth the effort.
  const { stateJSON, externalisedStateFields } = externalizeTransactionState(txState)
  // console.log(`externalisedStateFields=`, externalisedStateFields)

  // Call the LUA script
  let result
  try {
    const connection = await RedisLua.regularConnection()

    result = await connection.datp_startStep(
      // Keys
      stateKey,
      // Parameters
      mode,
      txId,
      owner,
      externalId,
      pipelineName,
      nodeGroup,
      stateJSON,
      eventJSON,
      event.f2i,
      delayBeforeQueueing,
      wakeSwitch,
      externalisedStateFields)
    // console.log(`pipelineName=`, pipelineName)
    // console.log(`datp_startStep returned`.red, result)

    // Put the extracted values back in (if required)
    de_externalizeTransactionState(txState, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, externalisedStateFields, 0)
    // console.log(`State afterwards=${txState.stringify()}`)

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
          delayBeforeQueueing: result[5],
        }
        console.log(`after datp_startStep():`, reply)
        return reply
    }
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_startStep]`)
    throw e
  }
}//- luaEnqueue_startStep
