/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
const { QueueItem } = require("./QueueItem")

export class Event extends QueueItem {
  #eventType
  #eventData

  /**
   *
   * @param {string} eventType
   * @param {XData} data
   */
  constructor(eventType, data) {
    super()
    // super({ eventType, data })
    this.#eventType = eventType
    this.#eventData = data
  }

  getType() {
    return this.#eventType
  }

  getData() {
    return this.#eventData
  }


  // toString() {
  get [Symbol.toStringTag]() {
    return `${this.#eventType}, ${JSON.stringify(this.#eventData, '', 2)}`
  }
}
