/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ResultReceiver from "./ResultReceiver"

/**
 * This module maintains a list of step types.
 */
class ResultReceiverRegister {
  #index

  constructor() {
    this.#index = [ ]
  }

  async register(name, resultReceiver) {
    // console.log(`ResultReceiverRegister.register(${name})`)
    // console.log(`resultReceiver=`, resultReceiver)
    // console.log(`typeof(resultReceiver)=`, typeof(resultReceiver))
    const chType = ResultReceiver.constructor.name
    // console.log(`chType=`, chType)

    if (!(resultReceiver instanceof ResultReceiver)) {
    // if (typeof(resultReceiver) !== 'function') {
      const msg = `resultReceiver parameter must be type ResultReceiver`
      // console.error(msg)
      throw new Error(msg)
    }
    if (typeof(resultReceiver.haveResult) !== 'function') {
      throw new Error(`ResultReceiver (${name}) must implement haveResult()`)
    }

    this.#index[name] = resultReceiver
  }

  async getHandler(name) {
    const resultReceiver = this.#index[name]
    if (!resultReceiver) {
      throw new Error(`Unknown ResultReceiver (${name})`)
    }
    return resultReceiver ? resultReceiver : null
  }


  async names() {
    const list = [ ]
    for (const name in this.#index) {
      // console.log(`-> ${name}`)
      list.push(name)
    }
    list.sort((a, b) => {
      if (a < b) return -1
      if (a > b) return +1
      return 0
    })
    return list
  }
}

const defaultRegister = new ResultReceiverRegister()
export default defaultRegister
