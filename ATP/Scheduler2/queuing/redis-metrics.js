/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { KEYSPACE_NODE_REGISTRATION, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_SLEEPING, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, KEYSPACE_TOARCHIVE, RedisLua, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3, VERBOSE } from './redis-lua';


export async function registerLuaScripts_metrics() {
  // console.log(`registerLuaScripts_metrics() `.magenta)
  const connection = await RedisLua.regularConnection()

  /*
    *  Get metrics
    */
  connection.defineCommand("datp_queueStats", {
    numberOfKeys: 0,
    lua: `

    -- Times and timeslots for metrics
    local nowArr = redis.call('TIME')
    local seconds = nowArr[1] + 0
    local millis = math.floor(nowArr[2] / 1000)
    local now = (seconds * 1000) + millis
    local timeslotA = math.floor(seconds / ${STATS_INTERVAL_2}) * ${STATS_INTERVAL_2}
    -- local expiry1 = timeslot1 + ${STATS_RETENTION_2}
    local statsKeyA = '${KEYSPACE_STATS_2}' .. timeslotA
    local timeslotB = timeslotA - ${STATS_INTERVAL_2}
    local statsKeyB = '${KEYSPACE_STATS_2}' .. timeslotB

    -- The current timeslot is not complete - work how much to use of previous timeslot to include.
    local r1 = timeslotA * 1000
    local r2 = (now - (timeslotA * 1000))
    local r3 = ${STATS_INTERVAL_2}
    local ratio = 1.0 - (now - (timeslotA * 1000)) / (${STATS_INTERVAL_2} * 1000)


    -- Get the nodes
    local nodes = redis.call('keys', '${KEYSPACE_NODE_REGISTRATION}*')
    local inputQueues = redis.call('keys', '${KEYSPACE_QUEUE_IN}*')
    local outputQueues = redis.call('keys', '${KEYSPACE_QUEUE_OUT}*')
    local adminQueues = redis.call('keys', '${KEYSPACE_QUEUE_ADMIN}*')

    -- Create a list of groups from the nodes
    local groups = { }
    for i = 1, #nodes do

      local node = nodes[i]
      local p1 = string.len("${KEYSPACE_NODE_REGISTRATION}")
      local p2 = string.find(node, ":", p1+1, true)
      local nodeGroup = string.sub(node, p1 + 1, p2 - 1)
      local nodeId = string.sub(node, p2 + 1, -1)
      -- local nodesForGroup = 
      if groups[nodeGroup] == nil then
        -- New group
        groups[nodeGroup] = { }
      end
    end

    -- Add any missing groups from queues
    for i = 1, #inputQueues do
      local q = inputQueues[i]
      local p1 = string.len("${KEYSPACE_QUEUE_IN}")
      local nodeGroup = string.sub(q, p1 + 1, -1)
      if groups[nodeGroup] == nil then
        -- New group
        groups[nodeGroup] = { }
      end
    end
    for i = 1, #outputQueues do
      local q = ouputQueues[i]
      local p1 = string.len("${KEYSPACE_QUEUE_IN}")
      local nodeGroup = string.sub(q, p1 + 1, -1)
      if groups[nodeGroup] == nil then
        -- New group
        groups[nodeGroup] = { }
      end
    end
    for i = 1, #adminQueues do
      local q = adminQueues[i]
      local p1 = string.len("${KEYSPACE_QUEUE_IN}")
      local nodeGroup = string.sub(q, p1 + 1, -1)
      if groups[nodeGroup] == nil then
        -- New group
        groups[nodeGroup] = { }
      end
    end

    -- Add stats to the groups
    for nodeGroup, v in pairs(groups) do
      -- Get the queued and the started values for this timeslot and the previous timeslot
      local fieldQueued = "g:::" .. nodeGroup .. ":::queued"
      local fieldStart = "g:::" .. nodeGroup .. ":::start"
      local dataA = redis.call('hmget', statsKeyA, fieldQueued, fieldStart)
      local queuedA = dataA[1]
      local startA = dataA[2]
      local dataB = redis.call('hmget', statsKeyB, fieldQueued, fieldStart)
      local queuedB = dataB[1]
      local startB = dataB[2]
      if not queuedA then
        queuedA = 0.0
      end
      if not queuedB then
        queuedB = 0.0
      end
      if not startA then
        startA = 0.0
      end
      if not startB then
        startB = 0.0
      end

      -- Take all in this (incomplete) timeslot, and part of the previous timeslot
      groups[nodeGroup][#groups[nodeGroup]+1] = "---STATS[" .. ${STATS_INTERVAL_2} .. " seconds]--- (queued/started)"
      groups[nodeGroup][#groups[nodeGroup]+1] = queuedA + (ratio * queuedB)
      groups[nodeGroup][#groups[nodeGroup]+1] = startA + (ratio * startB)
    end

    -- Add the queue sizes
    for nodeGroup, v in pairs(groups) do
      groups[nodeGroup][#groups[nodeGroup]+1] = "---QUEUES--- (in/out/admin)"
      -- input
      local kIn = '${KEYSPACE_QUEUE_IN}' .. nodeGroup
      groups[nodeGroup][#groups[nodeGroup]+1] = redis.call('LLEN', kIn)
      -- output
      local kOut = '${KEYSPACE_QUEUE_OUT}' .. nodeGroup
      groups[nodeGroup][#groups[nodeGroup]+1] = redis.call('LLEN', kOut)
      -- input
      local kAdmin = '${KEYSPACE_QUEUE_ADMIN}' .. nodeGroup
      groups[nodeGroup][#groups[nodeGroup]+1] = redis.call('LLEN', kAdmin)

      groups[nodeGroup][#groups[nodeGroup]+1] = "---NODES--- (id, json)"
    end


    -- Add the nodes to the groups
    for i = 1, #nodes do
      local node = nodes[i]
      local p1 = string.len("${KEYSPACE_NODE_REGISTRATION}")
      local p2 = string.find(node, ":", p1+1, true)
      local nodeGroup = string.sub(node, p1 + 1, p2 - 1)
      local nodeId = string.sub(node, p2 + 1, -1)
      groups[nodeGroup][#groups[nodeGroup]+1] = nodeId        
      local json = redis.call('GET', node)
      groups[nodeGroup][#groups[nodeGroup]+1] = json
    end

    -- Convert the associative array to an array with pairs of values
    local reply = {}
    reply[#reply+1] = "---LISTS--- (processing/webhook/archive)"
    reply[#reply+1] = redis.call('ZCOUNT', "${KEYSPACE_PROCESSING}", "-inf", "+inf")
    reply[#reply+1] = redis.call('ZCOUNT', "${KEYSPACE_SLEEPING}", "-inf", "+inf")
    reply[#reply+1] = redis.call('ZCOUNT', "${KEYSPACE_TOARCHIVE}", "-inf", "+inf")
    reply[#reply+1] = redis.call('ZCOUNT', "${KEYSPACE_SCHEDULED_WEBHOOK}", "-inf", "+inf")

    for k, v in pairs(groups) do
      reply[#reply+1] = k
      reply[#reply+1] = v
    end
    return reply
    `
  })//- datp_queueStats


  /*
    *  Get metrics
    */
  connection.defineCommand("datp_metrics", {
    numberOfKeys: 0,
    lua: `
    local prefix = ARGV[1]
    local withExtras = ARGV[2]
    local series = ARGV[3]

    -- Get the current time
    local nowArr = redis.call('TIME')
    local seconds = nowArr[1] + 0
    local millis = math.floor(nowArr[2] / 1000)
    local now = (seconds * 1000) + millis
    --log[#log+1] = 'Current time is ' .. now

    -- default to series 2
    local interval = ${STATS_INTERVAL_2}
    local retention = ${STATS_RETENTION_2}
    local keyspace = '${KEYSPACE_STATS_2}'
    if series == '1' then
      interval = ${STATS_INTERVAL_1}
      retention = ${STATS_RETENTION_1}
      keyspace = '${KEYSPACE_STATS_1}'
    elseif series == '3' then
      interval = ${STATS_INTERVAL_3}
      retention = ${STATS_RETENTION_3}
      keyspace = '${KEYSPACE_STATS_3}'
    end

    local currentTimeslot = math.floor(seconds / interval) * interval
    local numIntervals = math.floor(retention / interval)
    local endPeriod = currentTimeslot
    local startPeriod = endPeriod - (numIntervals * interval)


    local result = { startPeriod, interval, numIntervals, prefix, withExtras, series, '---' }

    -- Look at each timeslot
    for timeslot = startPeriod, endPeriod, interval do
      local key = keyspace .. timeslot

      local values = redis.call('hgetall', key)

      --if 1 == 1 then return result end

      local sample = { }
      for i = 1, #values, 2 do
        local name = values[i]
        local value = values[i+1]

        -- Do we need this field value?
        if prefix == '' then

          -- get the aggregated values only
          if name == 'queued' then
            sample[#sample+1] = name
            sample[#sample+1] = value
          elseif withExtras == '1' then
            if name:find('o:::', 1, true) == 1 then

              -- owner
              -- but exclude owner/pipeline
              if not name:find(':::p:::', 1, true) then
                sample[#sample+1] = name
                sample[#sample+1] = value
              end
            elseif name:find('p:::', 1, true) == 1 then

              -- pipeline
              sample[#sample+1] = name
              sample[#sample+1] = value
            elseif name:find('g:::', 1, true) == 1 then

              -- group
              sample[#sample+1] = name
              sample[#sample+1] = value
            elseif name:find('tx:::', 1, true) == 1 then

              -- transaction
              sample[#sample+1] = name
              sample[#sample+1] = value
            elseif name:find('iLen:::', 1, true) == 1 then

              -- input queue length
              sample[#sample+1] = name
              sample[#sample+1] = value
            elseif name:find('oLen:::', 1, true) == 1 then

              -- output queue length
              sample[#sample+1] = name
              sample[#sample+1] = value
            end
          end
        else

          -- Get requested fields only
          --sample[#sample+1] = name
          --sample[#sample+1] = 'WITH prefix'
          if name:find(prefix, 1, true) == 1 then
            sample[#sample+1] = name
            sample[#sample+1] = value
          end
        end
      end

      -- If we found some values, add this time period to the result
      if #sample > 0 then
        sample[#sample+1] = '-'
        sample[#sample+1] = timeslot
        result[#result+1] = sample
      end
      --local value = redis.call('hget', key, 'in')
      --if value then
      --  result[#result+1] = timeslot
      --  --result[#result+1] = key
      --  result[#result+1] = value
      --  result[#result+1] = '-'
      --end
    end
    return result
    `
  })//- datp_metrics
}


/**
 * Get queue statistics.
 * 
 * @returns 
 */
export async function luaQueueStats() {
  if (VERBOSE) console.log(`----- luaQueueStats()`.yellow)
  await RedisLua._checkLoaded()

  let result
  try {
    const connection = await RedisLua.regularConnection()
    result = await connection.datp_queueStats()
    // console.log(`datp_queueStats LUA: result=`, result)

    // result[0] is a header
    const processing = result[1]
    const sleeping = result[2]
    const archiving = result[3]
    const webhooks = result[4]
    // console.log(`processing=`, processing)
    // console.log(`sleeping=`, sleeping)
    // console.log(`archiving=`, archiving)
    // console.log(`webhooks=`, webhooks)

    const firstGroupIndex = 5

    // console.log(`result[firstGroupIndex]=`, result[firstGroupIndex])
    // console.log(`result[firstGroupIndex+1]=`, result[firstGroupIndex+1])

    const groups = [ ]
    for (let i = firstGroupIndex; i < result.length; i += 2) {
      const nodeGroup = result[i]
      const values = result[i+1]

      // console.log(`values for ${nodeGroup}:`)
      // for (let j = 0; j < 7; j++) {
      //   console.log(`- ${values[j]}`)
      // }

      const OFFSET_STATS = 0
      const OFFSET_QUEUES = 3
      const OFFSET_NODES = 7

      const nodes = [ ]
      for (let j = OFFSET_NODES + 1; j < values.length; j += 2) {
        const nodeId = values[j]
        const json = values[j + 1]
        try {
          const info = JSON.parse(json)
          delete info.nodeGroup
          delete info.stepTypes
          nodes.push(info)
        } catch (e) {
          // Bad JSON
          nodes.push({
            nodeId,
            workers: { }
          })
        }
      }
      groups.push({
        nodeGroup,
        stats: {
          queued: values[OFFSET_STATS + 1],
          started: values[OFFSET_STATS + 2],
        },
        queues: {
          in: values[OFFSET_QUEUES + 1],
          out: values[OFFSET_QUEUES + 2],
          admin: values[OFFSET_QUEUES + 3],
        },
        nodes
      })
    }

    // Sort the groups, with master first, and archiver and webhooks at the end.
    groups.sort((a, b) => {
      if (a.nodeGroup === 'master') return -1
      if (b.nodeGroup === 'master') return +1
      if (a.nodeGroup === 'webhooks') return +1
      if (b.nodeGroup === 'webhooks') return -1
      if (a.nodeGroup === 'archiver') return +1
      if (b.nodeGroup === 'archiver') return -1

      return a.nodeGroup.localeCompare(b.nodeGroup)
    })

    return {
      listSizes: {
        processing,
        sleeping,
        archiving,
        webhooks,
      },
      groups,
    }

  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_queueStats]`)
    throw e
  }
}//- luaQueueStats


/**
 * Get system metrics.
 * 
 * @returns 
 */
export async function luaGetMetrics(owner=null, pipeline=null, nodeGroup=null, series='1') {
  if (VERBOSE) console.log(`----- luaGetMetrics(${txId}, markAsReplied=${markAsReplied}, cancelWebhook=${cancelWebhook}))`.yellow)
  await RedisLua._checkLoaded()
  // const stateKey = `${KEYSPACE_STATE}${txId}`
  // const webhookKey = KEYSPACE_SCHEDULED_WEBHOOK

  // const METRICS_COMBINED = 1
  // const METRICS_COMBINED_PLUS = 1
  // const METRICS_COMBINED = 1
  // const METRICS_COMBINED = 1
  // const METRICS_COMBINED = 1

  let prefix = ''
  if (owner && pipeline) {
    prefix = `o:::${owner}:::p:::${pipeline}`
  } else if (owner) {
    prefix = `o:::${owner}`
  } else if (pipeline) {
    prefix = `p:::${pipeline}`
  } else if (nodeGroup) {
    prefix = `g:::${nodeGroup}`
  }
  // console.log(`prefix=`, prefix)

  let result
  try {
    const connection = await RedisLua.regularConnection()
    const withExtras = 1
    series = `${series}` // must be a string
    result = await connection.datp_metrics(prefix, withExtras, series)
    console.log(`datp_metrics LUA: result=`, result)
    return result
  } catch (e) {
    console.log(`FATAL ERROR calling LUA script [datp_metrics]`)
    throw e
  }
}//- luaGetMetrics
