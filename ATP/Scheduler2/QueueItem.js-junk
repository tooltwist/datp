/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

export class QueueItem {
  #value
  #next
  #previous
  #isInQueue

  constructor(value) {
    this.#value = value
  }

  __getNext() {
    return this.#next
  }

  __getPrevious() {
    return this.#previous
  }

  __setNext(next) {
    this.#next = next
  }

  __setPrevious(previous) {
    this.#previous = previous
  }

  __setIsInQueue(inQueue) {
    if (this.#isInQueue) {
      // Already in a queue
      if (inQueue) {
        throw new Error(`item is already in a queue`)
      }
    } else {
      // Not in a queue
      if (!inQueue) {
        throw new Error(`item is not in a queue`)
      }
    }
    this.#isInQueue = inQueue
  }

  // toString() {
  get [Symbol.toStringTag]() {
    return this.#value
  }
}
