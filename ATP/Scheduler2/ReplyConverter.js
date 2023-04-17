/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import assert from "assert"

const VERBOSE = 0

let replyConverter = null

// /**
//  * This function provides the default reply conversion functionality.
//  * @param {object} txSummary A transaction summary returned by Transaction.getSummary()
//  * @returns { httpStatus, reply }
//  */
// function defaultReplyConverter(txSummary) {
//   if (VERBOSE) console.log(`defaultReplyConverter()`, txSummary)
//   txSummary.metadata._yarp = 'yarp'
//   return { httpStatus: 201, reply: txSummary }
// }

/**
 * Tell DATP to pre-process all replies to the API caller using the
 * provided function. If the transaction reply is accessed via polling,
 * then `httpStatus` provides the API return status. If the transaction
 * reply is via webhook, then `httpStatus` is ignored.
 * 
 * @param {function} func For example:
 * 
 * ```javascript
function myReplyConverter(defaultReply) {
    const myReply = { ...defaultReply, happy: 'days' }
    return { httpStatus: 404, reply: myReply }
}
 * ```
 */
export function registerReplyConverter(func) {

  // Perform a test on the converter function.
  if (VERBOSE) console.log(`Performing test reply conversion.`)
  assert(typeof(func) === 'function')
  const dummySummary = {

  }
  const obj = func(dummySummary)
  if (obj.httpStatus !== null && typeof(obj.httpStatus) !== 'number') {
    throw new Error(`Reply converter does not return valid 'httpStatus'`)
  }
  if (typeof(obj.reply) !== 'object') {
    throw new Error(`Reply converter does not return valid reply'`)
  }

  // Okay, passed the assertions. We can use this function.
  replyConverter = func
}

/**
 * This function takes a transaction summary provided by
 * `Transaction.getSummary(tenant, txId)` and gives a
 * "replyConverter" function a chance to modify it.
 * 
 * The 
 * 
 * @param {object} reply Transaction status summary from
 * `Transaction.getSummary(tenant, txId)`
 * @returns { httpStatus, modifiedReply}
 */
export function convertReply(reply) {
  let httpStatus
  let error
  try {

    // Call the reply converter, if one is registered
    const obj = replyConverter ? replyConverter(reply) : { httpStatus: 200, reply }

    // Check the reply converter returned { httpStatus, reply }
    if (typeof(obj) !== 'object') {

      // Didn't return { ... }
      error = `invalid reply`
    } else {

      // Check it returned the two required values.
      httpStatus = obj.httpStatus
      reply = obj.reply
      // console.log(`RETURNED httpStatus=`, httpStatus)

      // Check if we have a valid httpStatus
      if (typeof(httpStatus) === 'undefined') {
        httpStatus = 200
      }
      else if (typeof(httpStatus) !== 'number') {
        error = `invalid 'httpStatus' returned`
      }

      // Check we got { ..., reply }
      if (typeof(reply) !== 'object') {
        error = `invalid 'reply' returned`
      }
    }
  } catch (e) {
    error = e.toString()
  }

  
  // If there was an error, mock up a reply containing the error.
  if (error) {
    const errReply = {
      metadata: {
        owner: reply.owner,
        "txId": reply.txId,
        "externalId": reply.externalId,
        "transactionType": reply.transactionType,
        "status": "error",
        error: `replyConverter: ${error}`,
        note: 'This does not indicate whether the transaction passed or failed.',
        "sequenceOfUpdate": 0,
      },
      "progressReport": null,
      "data": { }
    }
    return { httpStatus: 500, reply: errorReply }
  }

  // Reply converter worked okay.
  // console.log(`----`)
  // console.log(`httpStatus=`, httpStatus)
  // console.log(`reply=`, reply)
  return { httpStatus, reply }
}
