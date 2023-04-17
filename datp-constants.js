/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

import juice from '@tooltwist/juice-client'
// How long REDIS should store the key while checking for an externalId. This
// just needs to be long enough that we can be certain the database will have been
// written to all/any distributed copies of the database.
export const DUP_EXTERNAL_ID_DELAY = 60 // seconds

// Sleep intervals less than this length of time will be handled using setTimout()
// and sleeps longer than this time will be handled by our cron process.
export const DEEP_SLEEP_SECONDS = 2 * 60 // seconds


// Debugging database access
export const DEBUG_DB_ATP_TRANSACTION = 0
export const DEBUG_DB_ATP_TRANSACTION_DELTA = 0
export const DEBUG_DB_ATP_LOGBOOK = 0


// Options to bypass queueing if an event occurs on the same node.
export const SHORTCUT_STEP_START = true
export const SHORTCUT_STEP_COMPLETE = true
// export const SHORTCUT_TX_COMPLETION = false

// Check for steps/workers taking longer than X seconds
// (0 = no check, needs to be greater than DEEP_SLEEP_SECONDS)
export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = DEEP_SLEEP_SECONDS + 60
// export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = 1
// export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = 0

// If an event jumps to a new node, pass the state in the event.
export const INCLUDE_STATE_IN_NODE_HOPPING_EVENTS = true

// How often should we check the number of workers (ms)
export const WORKER_CHECK_INTERVAL = 15 * 1000

export const TEST_TENANT = 'acme'

/*
 *  Load config values using juice.
 */
let _loaded = false
let _development = false
let _logDestination = false

async function checkConfigLoaded() {
  if (_loaded) {
    return
  }
  _development = await juice.boolean('datp.development', false)
  _logDestination = await juice.string('datp.logDestination', 'db')
  _loaded = true
}

export async function isDevelopmentMode() {
  await checkConfigLoaded()
  return _development;
}

export async function logDestination() {
  await checkConfigLoaded()
  return _logDestination;
}
