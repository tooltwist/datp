/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { CHANNEL_NOTIFY, DELAY_BEFORE_ARCHIVING, EXTERNALID_UNIQUENESS_PERIOD, KEYSPACE_EXCEPTION, KEYSPACE_NODE_REGISTRATION, KEYSPACE_PROCESSING, KEYSPACE_QUEUE_ADMIN, KEYSPACE_QUEUE_IN, KEYSPACE_QUEUE_OUT, KEYSPACE_SCHEDULED_WEBHOOK, KEYSPACE_SLEEPING, KEYSPACE_STATE, KEYSPACE_STATS_1, KEYSPACE_STATS_2, KEYSPACE_STATS_3, KEYSPACE_TOARCHIVE, KEYSPACE_TOARCHIVE_LOCK1, KEYSPACE_TOARCHIVE_LOCK2, MAX_WEBHOOK_RETRIES, NUMBER_OF_EXTERNALIZED_FIELDS, PERSISTING_PERMISSION_LOCK1_PERIOD, PERSISTING_PERMISSION_LOCK2_PERIOD, PROCESSING_STATE_COMPLETE, PROCESSING_STATE_PROCESSING, PROCESSING_STATE_QUEUED, PROCESSING_STATE_SLEEPING, RedisLua, STATE_TO_REDIS_EXTERNALIZATION_MAPPING, STATS_INTERVAL_1, STATS_INTERVAL_2, STATS_INTERVAL_3, STATS_RETENTION_1, STATS_RETENTION_2, STATS_RETENTION_3, WEBHOOK_EXPONENTIAL_BACKOFF, WEBHOOK_INITIAL_DELAY } from './redis-lua';


/**
 * 
 * @param {*} callback 
 */
export async function luaListenToNotifications(callback) {
  console.log(`----- luaListenToNotifications()`)
  const connection = await RedisLua.adminConnection()
  try {
    connection.subscribe(CHANNEL_NOTIFY, (err, count) => {
      if (err) {
        // Just like other commands, subscribe() can fail for some reasons,
        // ex network issues.
        console.error("ERROR: Failed to subscribe: %s", err.message);
        console.log(` ✖ `.red + `unable to handle long poll replies`)
      } else {
        // `count` represents the number of channels this client are currently subscribed to.
        // console.log(
        //   `Subscribed successfully! This client is currently subscribed to ${count} channels.`
        // );
        console.log(` ✔ `.brightGreen + `long poll replies from this node`)
      }

      connection.on("message", (channel, message) => {
        // console.log(`Received ${message} from ${channel}`);
        const arr = message.split(':')
        const type = arr[0]
        const txId = arr[1]
        const status = arr[2]
        callback(type, txId, status)
      });
    });
  } catch (e) {
    console.log(`FATAL ERROR setting up pub/sub`)
    throw e
  }
}//- luaListenToNotifications

export async function luaNotify(txId) {
  const connection = await RedisLua.adminConnection()
  connection.publish(CHANNEL_NOTIFY, txId);
  console.log("Published %s to %s", txId, CHANNEL_NOTIFY);
}//- notify

