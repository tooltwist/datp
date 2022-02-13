/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'
import pause from '../../lib/pause'

const MAX_DELAY = 60 * 1000 // One minute
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
    if (this.#minDelay > MAX_DELAY) {
      this.#minDelay = MAX_DELAY
    }
    if (this.#maxDelay > MAX_DELAY) {
      this.#maxDelay = MAX_DELAY
    }
    if (this.#minDelay < 0) {
      this.#minDelay = 0
    }
    if (this.#maxDelay < this.#minDelay) {
      this.#maxDelay = this.#minDelay
    }
  }//- contructor

  async invoke(instance) {
    instance.trace(`RandomDelayStep (${instance.getStepId()})`)
    if (VERBOSE) {
      console.log(`RandomDelayStep (${instance.getStepId()})`)
    }

    const output = instance.getDataAsObject()
    const range = (this.#maxDelay - this.#minDelay)
    const delay = this.#minDelay + Math.floor(Math.random() * range)
    instance.trace(`Delay ${delay}ms (${this.#minDelay}ms - ${this.#maxDelay}ms)`)
    if (VERBOSE) {
      console.log(`Delay ${delay}ms (${this.#minDelay}ms - ${this.#maxDelay}ms)`)
    }

    pause(delay)
    const note = `${delay}ms`
    if (VERBOSE) {
      console.log(`Random delay completed`)
    }
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
  // console.log(`obj=`, obj)
  return obj
}//- factory

const myDef = {
  register,
  factory,
  defaultDefinition,
}
export default myDef
