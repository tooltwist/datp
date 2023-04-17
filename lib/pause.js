/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
/**
 * Pause execution for a while.
 * NOTE: Make sure you use 'await pause()'
 *
 * @param {integer} milliseconds
 * @returns null
 */
export default async function pause(milliseconds) {
  if (milliseconds <= 0) {
    return
  }
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve(null)
    }, milliseconds)
  })
}