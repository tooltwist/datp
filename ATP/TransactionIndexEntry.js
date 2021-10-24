/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

export default class TransactionIndexEntry {
  #txId
  #startTime
  #transactionType
  #status
  #initiatedBy
  #rootStepId
  // #stepStatus
  #color
  #initialData

  static RUNNING = 'running'
  static FROZEN = 'frozen'
  static HOLD = 'hold'
  static ROLLBACK = 'rollback'
  static ROLLBACK_COMPLETE = 'rollback_complete'
  static ROLLBACK_FAILED = 'rollback_failed'
  static COMPLETE = 'complete'
  static TERMINATED = 'terminated'
  static UNKNOWN = 'unknown'

  constructor(transactionId, transactionType, status, initiatedBy, initialTxData) {
    // console.log(`TransactionIndexEntry.constructor()`)
    // console.log(`transactionType=`, transactionType)
    // console.log(`initiatedBy=`, initiatedBy)
    // console.log(`initialTxData=`, initialTxData)
    this.#txId = transactionId
    // console.log(`this.#txId=`, this.#txId)

    this.#startTime = new Date()


    this.#initialData = initialTxData
    this.#transactionType = transactionType
    this.#status = status
    this.#initiatedBy = initiatedBy
    this.#rootStepId = null
    // this.#status = 'unknown'
    this.#color = 'black'
  }

  async getTxId() {
    return this.#txId
  }

  async getStartTime() {
    return this.#startTime
  }

  // async setTransactionType(transactionType) {
  //   this.#transactionType = transactionType
  // }

  async getTransactionType() {
    return this.#transactionType
  }

  async getTransactionStatus() {
    return this.#status
  }

  async getInitiatedBy() {
    return this.#initiatedBy
  }

  async setRootStepId(stepId) {
    this.#rootStepId = stepId
  }

  async getRootStepId() {
    return this.#rootStepId
  }

  async setStatus(status) {
    this.#status = status
  }

  async getStatus() {
    return this.#status
  }

  async getInitialData() {
    return this.#initialData
  }

  async setColor(color) {
    this.#color = color
  }

  async getColor() {
    return this.#color
  }

  // // Add into to toString() when debugging
  // // See https://stackoverflow.com/questions/42886953/whats-the-recommended-way-to-customize-tostring-using-symbol-tostringtag-or-ov
  // get [Symbol.toStringTag]() {
  //   return this.#txId
  // }

}