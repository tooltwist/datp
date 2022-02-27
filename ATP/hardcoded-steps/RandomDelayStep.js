/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'
import pause from '../../lib/pause'
import StepInstance from "../StepInstance"

const VERBOSE = 0

class RandomDelayStep extends Step {
  #minDelay
  #maxDelay

  constructor(definition) {
    super(definition)

    this.#minDelay = 500
    this.#maxDelay = 10000

    if (definition.min) {
      this.#minDelay = definition.min
    }
    if (definition.max) {
      this.#maxDelay = definition.max
    }
  }//- constructor

  /**
   * 
   * @param {StepInstance} instance 
   * @returns void
   */
  async invoke(instance) {
    // instance.trace(`RandomDelayStep (${instance.getStepId()})`)
    if (VERBOSE) {
      console.log(`RandomDelayStep (${instance.getStepId()})`)
    }

    // Deide how long to sleep
    const input = instance.getDataAsObject()
    if (input.delay) {
      // Specified the exact delay
      instance.trace(`Input overriding the delay in the step definition [${input.delay}]`)
      this.#minDelay = input.delay
      this.#maxDelay = input.delay
    }
    if (this.#minDelay < 0) {
      this.#minDelay = 0
    }
    if (this.#maxDelay < this.#minDelay) {
      this.#maxDelay = this.#minDelay
    }
    const range = (this.#maxDelay - this.#minDelay)
    const delayMs = this.#minDelay + Math.floor(Math.random() * range)
    const minmax = (this.#maxDelay === this.#minDelay) ? `` : `(${this.#minDelay}ms - ${this.#maxDelay}ms)`

    instance.trace(`Delay ${delayMs}ms ${minmax}`)

    // If this is the first time this step has been called, do the sleep.
    const counter = await instance.getRetryCounter()
    if (counter === 0) {

      if (VERBOSE) {
        console.log(`Delay ${delayMs}ms ${minmax}`)
      }
      await instance.syncLogs()

      
      if (delayMs < 10000) { // 10 seconds
        // Sleep using pause, which has millisecond resolution (sort of)
        instance.trace(`Will retry again in ${delayMs}ms`)
        await pause(delayMs)
      } else {
        // Sleep using retry, which will work via cron
        const delaySeconds = Math.round(delayMs / 1000)
        instance.trace(`Will retry again in ${delaySeconds} seconds`)
        return await instance.retryLater(null, delaySeconds)
      }
    }

    // Time to finish
    const note = `${delayMs}ms`
    if (VERBOSE) {
      console.log(`Random delay completed`)
    }
    const output = input
    return await instance.succeeded(note, output)
  }//- invoke
}//- class RandomDelayStep

async function register() {
  await StepTypes.register(myDef, 'util/delay', 'Delay a random period of time')
}//- register

async function defaultDefinition() {
  return {
    // description: 'Wait a random period of time',
    min: 1000,
    max: 5000,
  }
}

async function factory(definition) {
  const obj = new RandomDelayStep(definition)
  return obj
}//- factory

const myDef = {
  register,
  factory,
  defaultDefinition,
}
export default myDef
