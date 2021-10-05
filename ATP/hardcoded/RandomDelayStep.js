import Step from "../Step"
import StepTypes from '../StepTypeRegister'

const MAX_DELAY = 60 * 1000 // One minute
const VERBOSE = true

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
    if (VERBOSE) {
      // instance.console(`*****`)
      instance.console(`RandomDelayStep (${instance.getStepId()})`)
      // instance.console(`*****`)
      // console.log(`this=`, this)
      // console.log(`instance=`, instance)
    }

    const range = (this.#maxDelay - this.#minDelay)
    const delay = this.#minDelay + Math.floor(Math.random() * range)
    if (VERBOSE) {
      instance.console(`Delay ${delay}ms (${this.#minDelay}ms - ${this.#maxDelay}ms)`)
    }

    // logbook.log(this.stepId, `RandomDelayStep.invoke()`, {
    //   level: logbook.LEVEL_DEBUG,
    //   data: instance.data,
    // })

    setTimeout(() => {
      // instance.tx.said = 'nothing'
      const note = `${delay}ms`
      instance.finish(Step.COMPLETED, note, instance.getData())
    }, delay)
  }//- invoke

  // async getNote() {
  //   return 'NoNoye'
  // }
}//- class Dummy

async function register() {
  await StepTypes.register(myDef, 'delay', 'Delay a random period of time')
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
