/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

import { pipelineStepCompleteCallback, PIPELINE_STEP_COMPLETE_CALLBACK } from "./pipelineStepCompleteCallback"
import { childPipelineCompletionCallback, CHILD_PIPELINE_COMPLETION_CALLBACK } from "./ChildPipelineCompletionCallback"
import { txCompleteCallback, TX_COMPLETE_CALLBACK } from "./txCompleteCallback"
import assert from "assert"
import { GO_BACK_AND_RELEASE_WORKER } from "./Worker2"

const VERBOSE = 0

/**
 * This module maintains a list of step types.
 */
export default class CallbackRegister {
  static _index = null


  static _checkInitialized () {
    if (CallbackRegister._index === null) {
      CallbackRegister._index = [ ]

      // Add a few built-in callbacks
      CallbackRegister._index[PIPELINE_STEP_COMPLETE_CALLBACK] = pipelineStepCompleteCallback
      CallbackRegister._index[CHILD_PIPELINE_COMPLETION_CALLBACK] = childPipelineCompletionCallback
      CallbackRegister._index[TX_COMPLETE_CALLBACK] = txCompleteCallback
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

  static async call(tx, name, f2i, nodeInfo, worker) {
    if (VERBOSE) console.log(`% CallbackRegister.call(${name}, f2i=${f2i})`, nodeInfo)
    this._checkInitialized()

    assert(typeof(f2i) === 'number')

    const func = CallbackRegister._index[name]
    if (!func) {
      console.log(`Can't find callback ${name}`)
      for (let name in CallbackRegister._index) {
        console.log(`  -> ${name}`)
      }
      throw new Error(`Unknown callback [${name}]`)
    }
    const rv = await func(tx, f2i, nodeInfo, worker)
    // console.log(`name=`, name)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
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
