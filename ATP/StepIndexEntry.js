/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ResultReceiverRegister from './ResultReceiverRegister'
import assert from 'assert'
import TxData from './TxData'


export default class StepIndexEntry {
  #transactionId
  #parentStepId // String
  #stepId // String
  #fullSequence // String
  #stepType // String
  #description // String
  #resultReceiver // String
  #completionToken // String
  #completionHandlerData // String
  #status // String
  #note
  #preTx // [ name -> value]
  #postTx // [ name -> value]

  #instance // StepInstance

  #startTime // Date

  constructor(options) {
    this.#transactionId = options.transactionId
    this.#parentStepId = options.parentStepId
    this.#stepId = options.stepId
    this.#fullSequence = options.fullSequence
    this.#stepType = options.stepType
    this.#description = options.description
    this.#preTx = options.preTx
    this.#postTx = null

    // How to handle completion of this step
    this.#resultReceiver = options.resultReceiver
    this.#completionToken = options.completionToken // step completion must quote this
    this.#completionHandlerData = options.completionHandlerData

    this.#status = options.status
    this.#note = ''

    this.#instance = null

    this.#startTime = new Date()
  }

  async getTransactionId() {
    return this.#transactionId
  }

  async getStepId() {
    return this.#stepId
  }

  async getParentStepId() {
    return this.#parentStepId
  }

  async getFullSequence() {
    return this.#fullSequence
  }

  async getStepType() {
    return this.#stepType // String
  }

  async getDescription() {
    return this.#description
  }

  async getNote() {
    return this.#note
  }

  async getStartTime() {
    return this.#startTime
  }

  async validateToken(token) {
    if (token !== this.#completionToken) {
      throw new Error(`Invalid completionToken - possible hack attempt on step (${this.#stepId})?`)
    }
  }

  async callCompletionHandler(token, status, note, newTx) {
    // console.log(`callCompletionHandler(${token}, status=${status}, note=${note})`, newTx)
    // assert(token instanceof String)  ZZZZZ
    // assert(status instanceof String) ZZZZZ
    assert(newTx instanceof TxData)
    // console.log(`StepIndexEntry.callCompletionHandler(${token}, ${status}, ${note})`, newTx.toString())
    await this.validateToken(token)
    const completionHandlerObj = await ResultReceiverRegister.getHandler(this.#resultReceiver)
    await completionHandlerObj.haveResult(this.#completionHandlerData, status, note, newTx)
  }

  async getStatus() {
    return this.#status
  }

  async setStatus(token, status) {
    await this.validateToken(token)
    this.#status = status
  }

  async setNote(token, note) {
    await this.validateToken(token)
    this.#note = note
  }

  async setPostTx(token, newTx) {
    await this.validateToken(token)
    this.#postTx = newTx // ZZZZ Should copy the data, not reference
  }

  async setStepInstance(stepInstance) {
    this.#instance = stepInstance
  }

  async getStepInstance() {
    if (this.#instance === null) {
      throw new Error(`Instance has not been set (${this.#stepId})`)
    }
    return this.#instance
  }

  // Add into to toString() when debugging
  // See https://stackoverflow.com/questions/42886953/whats-the-recommended-way-to-customize-tostring-using-symbol-tostringtag-or-ov
  get [Symbol.toStringTag]() {
    return this.#stepId
  }
}