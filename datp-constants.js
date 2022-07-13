/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

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
export const SHORTCUT_STEP_START = false
export const SHORTCUT_STEP_COMPLETE = false
export const SHORTCUT_TX_COMPLETION = false

// Check for steps/workers taking longer than X seconds
// (0 = no check, needs to be greater than DEEP_SLEEP_SECONDS)
export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = DEEP_SLEEP_SECONDS + 60
// export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = 1
// export const CHECK_FOR_BLOCKING_WORKERS_TIMEOUT = 0

// If an event jumps to a new node, pass the state in the event.
export const INCLUDE_STATE_IN_NODE_HOPPING_EVENTS = true

// Should we save transacrion states (or just rely on the deltas?)
export const PERSIST_FINAL_TRANSACTION_STATE = false

// How often should we check the number of workers (ms)
export const WORKER_CHECK_INTERVAL = 30 * 1000
