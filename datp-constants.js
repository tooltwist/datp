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