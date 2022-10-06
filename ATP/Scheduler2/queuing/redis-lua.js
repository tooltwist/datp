import juice from '@tooltwist/juice-client'
import { schedulerForThisNode } from '../../..';
import query from '../../../database/query';
import pause from '../../../lib/pause';
import { STEP_TYPE_PIPELINE } from '../../StepTypeRegister';
import Transaction, { TX_STATUS_QUEUED, TX_STATUS_RUNNING } from '../Transaction';
const Redis = require('ioredis');

const VERBOSE = 0

// Only get the connection once
const CONNECTION_NONE = 0
const CONNECTION_WAIT = 1
const CONNECTION_READY = 2
let connectionStatus = CONNECTION_NONE

// Connections to REDIS
let connection_dequeue = null // mostly blocking, waiting on queue
let connection_enqueue = null
let connection_admin = null

/**
 * Event queues.
 * One of each per nodeGroup.
 * LIST
 */
const KEYSPACE_QUEUE_IN = 'qInL:'
const KEYSPACE_QUEUE_OUT = 'qOutL:'
const KEYSPACE_QUEUE_ADMIN = 'qAdmin:L'

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

/**
 * ZZZZ Do we need this?
 */
const KEYSPACE_SLEEPING = 'sleeping:'

/**
 * IDs of transactions that can be archived to long term storage.
 *  - A list of txIds that should be persisted to long term storage.
 *  - The transaction state remains in the State table.
 *  - Can be sleeping, or complete transactions.
 *  - A dedicated process saves to DB, then removes from State and toPersist.
 *  - Sorted list, with a ‘persist time’ used as the sort key.
 */
const KEYSPACE_TOARCHIVE = 'toArchive'

/**
 * IDs of completed transactions that need a reply via webhook.
 *  - txIds requiring a reply by webhook.- a dedicated process is used to handle webhook replies.
 *  - after a successful webhook reply, we mark the webhook status and add to the toPersist list.
 *  - sorted list, by add time.
 */
const KEYSPACE_WEBHOOK = 'webhooksH'

/**
 * Pipelne definitions
 */
const KEYSPACE_PIPELINE = 'pipelineH:'

/**
 * pub/sub channel for completed transactions and progress reports.
 */
const CHANNEL_NOTIFY = 'datp-notify'


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

    // const host = 'localhost'
    // const port = 6379
    // const password = null




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
      local webhookKey = KEYS[4]
      local runningKey = KEYS[5]

      local queueType = ARGV[1]
      local pipeline = ARGV[2]
      local nodeGroup = ARGV[3] -- Overridden if we have a pipeline.
      local eventJSON = ARGV[4]
      local stateJSON = ARGV[5]
      local txId = ARGV[6]
      local status = ARGV[7]
      local webhook = ARGV[8]
      local eventType = ARGV[9]

      -- Check this transaction is not already in the queue
      local existingStatus = redis.call('hget', stateKey, 'status')
      if existingStatus == '${TX_STATUS_QUEUED}' then
        local msg = 'Internal Error: datp_enqueue: Transaction is already queued'
        return { 'error', 'E99288', txId, msg }
      end

      --if nodeGroup == 'slave1' then
      --  return { 'debug', '0', pipeline, '-', pipelineKey, nodeGroup, 'END' }
      --end

      -- If a pipeline is specified, get the nodeGroup from the pipeline definition.
      local pipelineVersion = nil
      if pipeline ~= '' then  
        local details = redis.call('hmget', pipelineKey, 'version', 'nodeGroup')
        pipelineVersion = details[1]
        nodeGroup = details[2]
        if pipelineVersion == nil then
          local msg = 'Internal Error: datp_enqueue: Unknown pipeline [' .. pipeline .. ']'
          return { 'error', 'E982662', pipeline, msg, pipelineKey, 'END' }
        end

        --if nodeGroup == 'slave1' then
        --  return { 'debug', '-', pipeline, '-', pipelineKey, details, pipelineVersion, nodeGroup, 'END', yarp, queueKey }
        --end

        if queueType == 'in' then
          queueKey = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
        elseif queueType == 'out' then
          queueKey = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
        elseif queueType == 'admin' then
          queueKey = '${KEYSPACE_QUEUE_ADMIN}' .. nodeGroup
        end
      end

      -- Save the transaction state
      local nowArr = redis.call('TIME')
      local seconds = nowArr[1] + 0
      local millis = math.floor(nowArr[2] / 1000)
      local now = (seconds * 1000) + millis


      redis.call('hset', stateKey,
          'txId', txId,
          'status', '${TX_STATUS_QUEUED}',
          'event', eventJSON,
          'state', stateJSON,
          'eventType', eventType,
          'nodeGroup', nodeGroup,
          'pipeline', pipeline,
          'ts', now,
          'queue', queueKey)



      --if true then
      --return { 'debug', '3', pipeline, '-', pipelineKey, pipelineVersion, nodeGroup, 'END', queueKey, txId }
      --end


      -- Save the event
      --local queueKey2 = queueKey
      redis.call('rpush', queueKey, txId)
      --redis.call('rpush', queueKey2, txId)

      --if true then
      --return { 'debug', '4', pipeline, '-', pipelineKey, pipelineVersion, nodeGroup, 'END', queueKey }
      --end
      
      -- Save the webhook, if there is one.
      if webhook ~= '' then
        redis.call('hset', webhookKey, txId, webhook)
      end

      --if true then
      --return { 'debug', '5', pipeline, '-', pipelineKey, pipelineVersion, nodeGroup, 'END', queueKey }
      --end
      
      -- Remove it from the 'processing' list
      local stateKey = 'stateH:' .. txId
      redis.call('zrem', runningKey, txId)

      --if true then
      --return { 'debug', '6', pipeline, '-', pipelineKey, pipelineVersion, nodeGroup, 'END', queueKey }
      --end
      
      -- return { '0.0', txId, status, stateJSON }
      return { 'ok', queueKey, nodeGroup, pipeline, pipelineVersion, webhook }
    `});

    /*
     *  Get a transaction state.
     */
    connection_enqueue.defineCommand("datp_getState", {
      numberOfKeys: 1,
      lua: `
      local stateKey = KEYS[1]
      return redis.call('hmget', stateKey, 'txId', 'status', 'event', 'state', 'eventType', 'pipeline', 'ts', 'queue')
    `})

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
        local required = tonumber(ARGV[1])

        -- Look in the admin queue first
        local list = { }
        local nowArr = redis.call('TIME')
        local seconds = nowArr[1] + 0
        local millis = math.floor(nowArr[2] / 1000)
        local now = (seconds * 1000) + millis
        -- local now = redis.call('TIME')[1] + 0
        if required > 0 then
          local events = redis.call('LPOP', qAdminKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              local txId = events[i]
              local stateKey = 'stateH:' .. txId
              local txState = redis.call('hmget', stateKey, 'status', 'event', 'state', 'pipeline', 'eventType')
              local status = txState[1]
              local event = txState[2]
              local state = txState[3]
              local pipeline = txState[4]
              local eventType = txState[5]

              if not status then
                -- Event is in queue, but we don't have a status!
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Internal Error: Transaction in event queue has no state saved [' .. txId .. ']'
                list[#list + 1] = { 'warning', 'E37726', txId, msg }
              elseif status == 'queued' then
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
                list[#list + 1] = { 'event', txId, '${TX_STATUS_RUNNING}', event, state, qAdminKey, now, pipeline, pipelineDefinitionJSON, 'END' }
                redis.call('hset', stateKey, 'status', '${TX_STATUS_RUNNING}', 'queue', '')
                redis.call('zadd', runningKey, now, txId)
                required = required - 1
              elseif status == 'cancelled' then
                -- Ignore cancelled transactions
              else
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Transaction had status changed to [' .. status .. '] while in the admin queue'
                list[#list + 1] = { 'warning', 'E8827736', txId, msg }
              end
            end
          end
        end

        -- Look in the output queue next
        if required > 0 then
          local events = redis.call('LPOP', qOutKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              local txId = events[i]
              local stateKey = 'stateH:' .. txId
              local txState = redis.call('hmget', stateKey, 'status', 'event', 'state', 'pipeline', 'eventType')
              local status = txState[1]
              local event = txState[2]
              local state = txState[3]
              local pipeline = txState[4]
              local eventType = txState[5]

              if not status then
                -- Event is in queue, but we don't have a status!
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Internal Error: Transaction in event queue has no state saved [' .. txId .. ']'
                list[#list + 1] = { 'warning', 'E37726', txId, msg }
              elseif status == 'queued' then
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
                list[#list + 1] = { 'event', txId, '${TX_STATUS_RUNNING}', event, state, qOutKey, now, pipeline, pipelineDefinitionJSON, 'END' }
                redis.call('hset', stateKey, 'status', '${TX_STATUS_RUNNING}', 'queue', '')
                redis.call('zadd', runningKey, now, txId)
                required = required - 1
              elseif status == 'cancelled' then
                -- Ignore cancelled transactions
              else
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Transaction had status changed to [' .. status .. '] while in output queue [' .. qOutKey .. ']'
                list[#list + 1] = { 'warning', 'E8827736', txId, msg }
              end
            end
          end
        end

        -- Finally, look in the input queue
        if required > 0 then
          local events = redis.call('LPOP', qInKey, required)
          if events then
            -- Add non-cancelled events to our output
            for i = 1, #events do
              local txId = events[i]
              local stateKey = 'stateH:' .. txId
              local txState = redis.call('hmget', stateKey, 'status', 'event', 'state', 'pipeline', 'eventType')
              local status = txState[1]
              local event = txState[2]
              local state = txState[3]
              local pipeline = txState[4]
              local eventType = txState[5]

              if not status then
                -- Event is in queue, but we don't have a status!
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Internal Error: Transaction in event queue has no state saved [' .. txId .. ']'
                list[#list + 1] = { 'warning', 'E37726', txId, msg }
              elseif status == 'queued' then
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
                list[#list + 1] = { 'event', txId, '${TX_STATUS_RUNNING}', event, state, qInKey, now, pipeline, pipelineDefinitionJSON, 'END' }
                redis.call('hset', stateKey, 'status', '${TX_STATUS_RUNNING}', 'queue', '')
                redis.call('zadd', runningKey, now, txId)
                required = required - 1
              elseif status == 'cancelled' then
                -- Ignore cancelled transactions
              else
                -- This should not happen. Someone has changed the status while it is in the queue.
                local msg = 'Transaction had status changed to [' .. status .. '] while in input queue [' .. qInKey .. ']'
                list[#list + 1] = { 'warning', 'E8827736', txId, msg }
              end
            end
          end
        end

        return list
    `})

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

        -- Check the existing status of the transaction
        local existingValues = redis.call('hmget', stateKey, 'status', 'webhook')
        local status = existingValues[1]
        local webhook = existingValues[2]
        if not status then
          local msg = 'Internal Error: Transaction to be completed has no state saved [' .. txId .. ']'
          return { 'error', 'E662622', txId, msg }
        end
        -- ZZZ Check the status allows completion

        -- Update the status

        -- Publish notification of the transaction completion
        local message = 'completed:' .. txId .. ':' .. status
        redis.call('publish', '${CHANNEL_NOTIFY}', message )
        -- console.log("Published %s to %s", txId, CHANNEL_NOTIFY);
    
        -- Schedule any webhook to be called
        return { 'ok', txId, status, webhook }
    `})

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
   * @param {string} pipeline 
   * @param {object} event
   */
  async enqueue(queueType, pipeline, event) {
    // console.log(`----- enqueue(queueType=${queueType}, pipeline=${pipeline}, event)`)
    await RedisLua._checkLoaded()

    const originalTxState = event.txState
    const eventType = event.eventType


    //YARPLUA
    const txState = (typeof(originalTxState) === 'string') ? Transaction.transactionStateFromJSON(event.txState) : event.txState
    // console.log(`txState=`, txState)

    // const nodeGroup = event.nodeGroup
    // let nodeGroup = schedulerForThisNode.getNodeGroup()
    let nodeGroup = '-'
    if (pipeline) {
      // The LUA script will get the nodeGroup from the pipeline definition.
    } else {
      // This isn't a pipeline, so it wasn't queued. It will run on the current nodeGroup.
      nodeGroup = schedulerForThisNode.getNodeGroup()
      // Get the nodeGroup from the step definition
      // nodeGroup = txState.getNodeGroupForStep(event.stepId)
    }


    const txId = txState.getTxId()
    const status = 'queued'
    // delete state.status

    const transactionData = txState.transactionData()
    // console.log(`transactionData=`, transactionData)
    if (!transactionData.metadata) {
      throw new Error(`Missing event.txState.transactionData.metadata`)
    }
    const metadata = transactionData.metadata
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

    const tmpTxState = event.txState
    delete event.txState
    const eventJSON = JSON.stringify(event)
    event.txState = tmpTxState

    const pipelineKey = pipeline ? `${KEYSPACE_PIPELINE}${pipeline}` : `${KEYSPACE_PIPELINE}-`
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const stateJSON = txState.stringify()
    const webhookKey = KEYSPACE_WEBHOOK
    const runningKey = `${KEYSPACE_RUNNING}`

    //YARPLUA
    event.txState = originalTxState

    // lua_enqueue(txId, status, webhook, stateJSON, eventJSON)

    console.log(`CALLING datp_enqueue`)
    console.log(`pipelineKey=`, pipelineKey)
    console.log(`queueKey=`, queueKey)
    console.log(`stateKey=`, stateKey)
    console.log(`webhookKey=`, webhookKey)
    console.log(`runningKey=`, runningKey)
    console.log(`queueType=`, queueType)
    console.log(`pipeline=`, pipeline)
    console.log(`nodeGroup=`, nodeGroup)
    console.log(`eventJSON=`, eventJSON)
    console.log(`stateJSON=`, stateJSON)
    console.log(`txId=`, txId)
    console.log(`status=`, status)
    console.log(`webhook=`, webhook)
    console.log(`eventType=`, eventType)
    const result = await connection_enqueue.datp_enqueue(
      // Keys
      pipelineKey, queueKey, stateKey, webhookKey, runningKey,
      // Parameters
      queueType, pipeline, nodeGroup,
      eventJSON, stateJSON, txId, status, webhook, eventType)
    console.log(`datp_enqueue returned`, result)

    // console.log(`all is okay here`)
    // await pause(3000)
    // console.log(`still okay`)

    if (result[0] === 'error') {
      const msg = `${result[1]}: ${result[2]}: ${result[3]}`
      console.log(msg)
      throw new Error(msg)
    } else if (result[0] === 'ok') {
      const reply = {
        queue: result[1],
        nodeGroup: result[2],
        pipelineName: result[3],
        pipelineVersion: result[4],
        webhook: result[5] ? result[5] : null
      }
      // console.log(`reply from enqueue =`, reply)
      return reply
    }
    // console.log(`-----`)
  }

  /**
   * Get the status of a transaction.
   * 
   * @param {string} txId 
   * @returns 
   */
  async getState(txId) {
    console.log(`----- getState(${txId})`)
    await RedisLua._checkLoaded()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const result = await connection_enqueue.datp_getState(stateKey)
    // console.log(`result=`, result)

    // 'txId', 'status', 'event', 'state', 'eventType', 'pipeline', 'ts')

    const txId2 = result[0]
    if (txId2 === null) {
      // Not found
      return null
    }
    const status = result[1]
    const eventJSON = result[2]
    const stateJSON = result[3]
    const eventType = result[4]
    const pipeline = result[5]
    const ts = parseInt(result[6])
    const queue = result[7]
    
    const event = JSON.parse(eventJSON)
    event.ts = ts

    const txState = JSON.parse(stateJSON)
    txState.txId = txId
    txState.status = status

    return {
      txId,
      event,
      txState,
      pipeline,
      ts,
      queue
    }
  }

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
  async transactionCompleted(txId, status) {
    console.log(`----- transactionCompleted(${txId}, ${status})`)
    if (status !== 'success' && status!=='failed' && status!=='aborted' && status!=='internal-error') {
      throw new Error('Invalid completion status')
    }
    await RedisLua._checkLoaded()
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const webhookKey = `${KEYSPACE_WEBHOOK}`
    const runningKey = `${KEYSPACE_RUNNING}`
    const persistKey = `${KEYSPACE_TOARCHIVE}`
    const result = await connection_enqueue.datp_completeTransaction(stateKey, webhookKey, runningKey, persistKey, txId, status)
    // console.log(`result=`, result)
    return result
  }

  async deleteZZZ(txId) {
    const stateKey = `${KEYSPACE_STATE}${txId}`
    const result = await connection_enqueue.hdel(stateKey, 'txId', 'status', 'ts', 'state', 'event', 'pipeline', 'eventType', 'queue')
    console.log(`result=`, result)
  }

  /**
   * Get events from the queues for a nodeGroup.
   * 
   * @param {string} nodeGroup 
   * @param {number} numEvents 
   * @returns 
   */
  async getEvents(nodeGroup, numEvents) {
    // console.log(`----- getEvents(${nodeGroup}, ${numEvents})`)

    await RedisLua._checkLoaded()
    const qInKey = `${KEYSPACE_QUEUE_IN}${nodeGroup}`
    const qOutKey = `${KEYSPACE_QUEUE_OUT}${nodeGroup}`
    const qAdminKey = `${KEYSPACE_QUEUE_ADMIN}${nodeGroup}`
    const runningKey = `${KEYSPACE_RUNNING}`
    const pipelinesKey = `${KEYSPACE_PIPELINE}`
    // console.log(`getting ${numEvents} events`)

    // console.log(`CALLING datp_dequeue`)
    // if (nodeGroup === 'slave1') {
    // console.log(`----- getEvents(${nodeGroup}, ${numEvents})`)
    // console.log(`qInKey=`, qInKey)
    //   console.log(`qOutKey=`, qOutKey)
    //   console.log(`qAdminKey=`, qAdminKey)
    //   console.log(`runningKey=`, runningKey)
    //   console.log(`pipelinesKey=`, pipelinesKey)
    // }
    const result = await connection_enqueue.datp_dequeue(qInKey, qOutKey, qAdminKey, runningKey, pipelinesKey, numEvents)
    // if (nodeGroup === 'slave1') {
      if (result.length > 0) {
      //   console.log(`----- getEvents(${nodeGroup}, ${numEvents})`)
        console.log(`from lua datp_dequeue =>`, result)
      }
      // console.log(`----- getEvents END`)
    //   // await pause(5000)
    // }

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
          /*
            [
              'event',
              '999',
              'running',
              '{"eventType":"start-pipeline","nodeGroup":"abc"}',
              '{"txId":"999","metadata":{"webhook":"http://localhost:3000/webhook"},"data":{"some":"stuff"}}',
              'qIn:abc',
              1664119932026,
              'test-pipeline',
              ''
            ]
          */
          const txId = values[1]
          const status = values[2]
          const eventJSON = values[3]
          const stateJSON = values[4]
          const queue = values[5]
          const ts = values[6]
          const pipeline = values[7]
          const pipelineDefinitionJSON = values[8]

          const event = JSON.parse(eventJSON)
          event.ts = ts
          // const txState = JSON.parse(stateJSON)
          const txState = Transaction.transactionStateFromJSON(stateJSON)
          // txState.txId = txId
          // txState.status = status
          // console.log(`getEvents txState=`, txState.stringify())
          // console.log(`getEvents DO WE NEED TO PATCH IN VALUES?`)
          // console.log(`getEvents ${txState.getTxId()} versus ${txId}`)
          // console.log(`getEvents ${txState.getStatus()} versus ${status}`)
          txState._patchInStatus(status)
          // console.log(`getEvents ${txState.getStatus()} versus ${status}`)
          event.txState = txState
          // console.log(`getEvents event=`, event)

          if (pipelineDefinitionJSON && pipelineDefinitionJSON !== '-') {
            // console.log(`\n\nPATCH IN PIPELINE DEFINITION`)
            const stepId = event.stepId
            // console.log(`getEvents stepId=`, stepId)
            const step = txState.stepData(stepId)
            // console.log(`getEvents step=`, step)

            // console.log(`getEvents ${step.stepDefinition} versus ${pipeline}`)
            // console.log(`getEvents pipelineDefinitionJSON=`, pipelineDefinitionJSON)
            step.vogStepDefinition = {
              stepType: STEP_TYPE_PIPELINE,
              description: `Pipeline ${pipeline}`,
              steps: JSON.parse(pipelineDefinitionJSON)
            }
            // console.log(`getEvents step.stepDefinition=`, step.stepDefinition)
          }
          console.log(`getEvents event=`, event)
          // console.log(`getEvents event.txState=`, event.txState.stringify())
          console.log(`          event.txState=`, JSON.stringify(event.txState.asObject(), '', 2))



          events.push(event)
          // events.push({
          //   event,
          //   txState,
          //   pipeline,
          //   pipelineDefinition,
          //   queue
          // })
          break
      }//- switch
    }//- for
    // if (events.length > 0) {
    //   console.log(`events=`, events)
    //   console.log(`----- getEvents returning`)
    // }
    return events
  }//- getEvents

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
      console.log(`Received ${message} from ${channel}`);
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
