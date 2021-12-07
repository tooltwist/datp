/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

/**
 * This module maintains a list of step types.
 */
class CallbackRegister {
  #index

  constructor() {
    this.#index = [ ]
  }

  async register(name, func) {
    if (typeof(name) !== 'string') {
      throw new Error(`CallbackRegister.register: name should be a string`)
    }
    if (typeof(func) !== 'function') {
      throw new Error(`CallbackRegister.register: func should be a function`)
    }

    this.#index[name] = func
  }

  async call(name, data) {
    const func = this.#index[name]
    if (!func) {
      throw new Error(`Unknown callback [${name}]`)
    }
    await func(data)
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

export default CallbackRegister = new CallbackRegister()
