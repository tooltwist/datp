/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import assert from 'assert';
import { STEP_TYPE_PIPELINE } from '../../StepTypeRegister';
import { DEFINITION_STEP_COMPLETE_EVENT, DEFINITION_PROCESS_STEP_START_EVENT, STEP_DEFINITION, validateStandardObject } from '../eventValidation';
import Scheduler2 from '../Scheduler2';
import { FLOW_PARANOID, KEYSPACE_PIPELINE, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, NUMBER_OF_EXTERNALIZED_FIELDS, NUMBER_OF_READONLY_FIELDS, PROCESSING_STATE_CANCELLED, PROCESSING_STATE_PROCESSING, PROCESSING_STATE_QUEUED, RedisLua, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATE_TO_REDIS_READONLY_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3, transactionStateFromJsonAndExternalizedFields } from './redis-lua';


export async function registerLuaScripts_dequeue() {
  // console.log(`registerLuaScripts_dequeue() `.magenta)
  const connection = await RedisLua.regularConnection()

    /*
     *  Get events from queues
     *  1. Get events from admin, in and out queues, in that order.
     * 
     */
    connection.defineCommand("datp_dequeue", {
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
        log[#log+1] = ' - change the processingState to processing'
        log[#log+1] = ' - add it to the list of events being processed'

        for i = 1, #txIds do
          local txId = txIds[i]
          local stateKey = '${KEYSPACE_STATE}' .. txId
          local txStateValues = redis.call('hmget', stateKey,
              'owner',
              'processingState',
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
            return { 'error', 'Internal error #912412: Incorrect number of parameters' }
          end
          if ${NUMBER_OF_READONLY_FIELDS} ~= 8 then
            return { 'error', 'Internal error #992407: Incorrect number of readonly parameters' }
          end
  
          local owner = txStateValues[1]
          local processingState = txStateValues[2]
          local pipeline = txStateValues[5]
  
          -- Decide what to do
          if not processingState then

            -- Event is in queue, but we don't have a processingState. ERROR!
            -- This should not happen. Someone has changed the processingState while it is in the queue.
            local msg = 'Internal Error: Transaction in event queue has no state saved [' .. txId .. ']'
            list[#list + 1] = { 'error', 'E37726', txId, msg }
          elseif processingState == '${PROCESSING_STATE_QUEUED}' then

            -- We have an event to process
            -- Perhaps get the pipeline definition
            -- Add it to our event list
            -- Change the processingState to 'processing'
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

            -- Set the processingState to 'processing'
            redis.call('hset', stateKey, 'processingState', '${PROCESSING_STATE_PROCESSING}', 'queue', '')

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


          elseif processingState == '${PROCESSING_STATE_CANCELLED}' then

        
            -- Ignore cancelled transactions
          else

            -- This should not happen. Someone has changed the processingState while it is in the queue. ERROR!
            local msg = 'Transaction had processingState changed to [' .. processingState .. '] while in output queue [' .. qOutKey .. ']'
            list[#list + 1] = { 'error', 'E8827736b', txId, msg }
          end
        end

        -- Add the log to the result, and return
        list[#list+1] = { 'log', log }
        return list
      `
    })//- datp_dequeue
}//- registerLuaScripts_dequeue


  /**
   * Get events from the queues for a nodeGroup.
   * 
   * @param {string} nodeGroup 
   * @param {number} numEvents 
   * @returns 
   */
export async function luaDequeue(nodeGroup, numEvents) {

  const qInKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`
  const qOutKey = `${KEYSPACE_QUEUE_OUT}${nodeGroup}`
  const qAdminKey = `${KEYSPACE_QUEUE_ADMIN}${nodeGroup}`
  const runningKey = `${KEYSPACE_PROCESSING}`
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
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_dequeue(qInKey, qOutKey, qAdminKey, runningKey, pipelinesKey, nodeGroup, numEvents)
    if (result.length > 1) { // Always have log
      // if (FLOW_VERBOSE)
      // console.log(`----- luaDequeue(${nodeGroup}, ${numEvents})`.gray)
      // console.log(`result=`, result)
    }
    // if (result.length > 1) { // Always have log
    //   console.log(`----- luaDequeue(${nodeGroup}, ${numEvents})`.yellow)
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
        // console.log(`luaDequeue(): values=`, values)
        // This is a valid event
        const txStateValues = values[1]
//           console.log(`txStateValues=`, txStateValues)
// console.log(`ZZZZZZZZZZZZZZZZZZZZZZZZZZZ`.magenta)

        // Get the txState details selected from REDIS
        const owner = txStateValues[0]
        const status = txStateValues[1]
        const eventJSON = txStateValues[2]
        // console.log(`eventJSON=`, eventJSON)
        const stateJSON = txStateValues[3]
        // console.log(`stateJSON=${stateJSON}`.brightBlue)
        const pipelineName = txStateValues[4]
        const firstExternalizedFieldIndex = 5
        const firstReadonlyFieldIndex = firstExternalizedFieldIndex + NUMBER_OF_EXTERNALIZED_FIELDS
        const extraFieldsIndex = firstReadonlyFieldIndex + NUMBER_OF_READONLY_FIELDS

        // Then the externalised fields
        const pipelineDefinitionJSON  = txStateValues[extraFieldsIndex]
        // console.log(`pipelineDefinitionJSON=`, pipelineDefinitionJSON)

        // Create the event and the transaction state.
        const event = JSON.parse(eventJSON)
        const txState = transactionStateFromJsonAndExternalizedFields(stateJSON, txStateValues, firstExternalizedFieldIndex, firstReadonlyFieldIndex)

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
