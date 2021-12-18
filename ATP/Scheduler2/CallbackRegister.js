/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

import { pipelineStepCompleteCallback, PIPELINE_STEP_COMPLETE_CALLBACK } from "./pipelineStepCompleteCallback"
import { rootStepCompleteCallback, ROOT_STEP_COMPLETE_CALLBACK } from "./rootStepCompleteCallback"

const VERBOSE = 0

/**
 * This module maintains a list of step types.
 */
export default class CallbackRegister {
  static _index = null

  // constructor() {
  //   console.log(`\n\n\n\n\n\n\n\n\n\n\n\n   ==================================== NEW CallbackRegister ====================================`)
  //   console.log(new Error().stack);
  // }

  static _checkInitialized () {
    if (CallbackRegister._index === null) {
      CallbackRegister._index = [ ]

      // Add a few built-in callbacks
      CallbackRegister._index[ROOT_STEP_COMPLETE_CALLBACK] = rootStepCompleteCallback
      CallbackRegister._index[PIPELINE_STEP_COMPLETE_CALLBACK] = pipelineStepCompleteCallback
    }
  }

  static async register(name, func) {
    if (VERBOSE) { console.log(`% CallbackRegister.register(${name})`)}
    this._checkInitialized()
    if (typeof(name) !== 'string') {
      throw new Error(`CallbackRegister.register: name should be a string`)
    }
    if (typeof(func) !== 'function') {
      throw new Error(`CallbackRegister.register: func should be a function`)
    }

    CallbackRegister._index[name] = func
  }

  static async call(name, context, nodeInfo) {
    if (VERBOSE) { console.log(`% CallbackRegister.call(${name})`, context, nodeInfo)}
    this._checkInitialized()

    const func = CallbackRegister._index[name]
    if (!func) {
      console.log(`Can't find callback ${name}`)
      for (let name in CallbackRegister._index) {
        console.log(`  -> ${name}`)
      }
      throw new Error(`Unknown callback [${name}]`)
    }
    await func(context, nodeInfo)
  }


  static async names() {
    this._checkInitialized()
    const list = [ ]
    for (const name in CallbackRegister._index) {
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
