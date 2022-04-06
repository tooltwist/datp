/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
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