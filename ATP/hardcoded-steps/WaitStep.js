/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import pause from "../../lib/pause"
import Step from "../Step"
import StepTypes from '../StepTypeRegister'

/*
 *  There is a potential concurrency problem in the following scenario:
 *  1. The step asks for a switch value.
 *  2. Some other code sets the switch value.
 *  3. The step calls instance.retry to wait for the step to change value.
 *  4. The previous change is not recognised, so the step does not retry.
 * 
 *  We handle this with "unacknowledged switch values". These appear in the
 *  switches ass a switch name prefixed by !, and the LUA retry logic takes
 *  them into account. See README-switches.md for details.
 * 
 *  If we set this value below, this step pauses long enough that we can test
 *  (2) above, but setting the switch value from MONDAT. If we are not testing,
 *  this flag should not be set.
 */
const TESTING_UNACKNOWLEDGED_SWITCH_CHANGES = false

const VERBOSE = 1

/**
 * This class represents a type of step, not an actual instance of a step
 * within a pipeline. In your application code you will need to call the
 * 'register' function below to register the step type. It can then be used
 * in pipelines.
 *
 * When a pipeline needs an actual step of this type, it will call the
 * 'factory' function. Note that the pipeline passes a definition to this
 * factory, so not all instances of this step type will be the same - you
 * can write this step to perform however you like, based upon the definition
 * it receives.
 *
 * When it is time to run the step, the pipeline will call the 'invoke'
 * function. When the step has completed running, it should call
 * instance.succeeded, instance.failed, etc.
 *
 * For long running options, the invoke function may return before the
 * step has completed, but some other part of your server will need to later
 * tell the Scheduler that the step has completed. See ZZZZ for more information.
 */
class WaitStep extends Step {
  #switch

  constructor(definition) {
    super(definition)
    this.#switch = definition.switch
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    if (VERBOSE) console.log(`-----------------------------------`.green)
    if (VERBOSE) console.log(`WaitStep START`.green)

    // Do something here
    const input = instance.getDataAsObject()
    if (VERBOSE) console.log(`Switch name is ${this.#switch}`.green)

    const { value } = await instance.getSwitch(this.#switch)

    instance.trace(`switch value=`, value)
    if (VERBOSE) console.log(`Switch value is ${value}`.green)

    // switch is set, we can proceed to the next step
    if (value) {
      if (VERBOSE) console.log(`MOVE ON TO THE NEXT STEP`.green)
      // Time to complete the step and send a result
      const note = `SWITCH ${this.#switch} IS SET - PROCEED TO THE NEXT STEP`
      instance.trace(note)
      const output = { ...input }
      delete output.instruction // ZZZZZ should leave output as null, for passthrough
      delete output.url
      delete output.qrcode
      return await instance.succeeded(note, output)
    } else {
      if (VERBOSE) console.log(`SWITCH HAS NO VALUE...`.green)
      if (VERBOSE) console.log(`NEED TO WAIT FOR SWITCH TO BE SET`.green)
      instance.trace(`NEED TO WAIT FOR SWITCH ${this.#switch}`)

      if (TESTING_UNACKNOWLEDGED_SWITCH_CHANGES) {
        console.log(`PAUSING, TO GIVE TIME TO CHANGE SWITCH VALUE IN MONDAT`.bgMagenta)
        await pause(20000)
        console.log(`CONTINUING`.bgMagenta)
      }
      return await instance.retryLater(this.#switch)
    }
  }

  // /**
  //  * This function is called to roll back anything done when invoke was run.
  //  * The step instance parameter provides the context of the transaction
  //  * and also convenience functions.
  //  * @param {StepInstance} instance
  //  */
  //  async rollback(instance) {
  //   instance.trace(`WaitStep rolling back (${instance.getStepId()})`)
  //   const data = instance.getDataAsObject()

  //   // Do something here
  //   //...

  //   // Time to complete the step and send a result
  //   const note = ''
  //   instance.succeeded(note, data)
  // }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'util/wait', 'Wait for a switch to be activated')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    "switch": "name-of-switch"
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new WaitStep(definition)
  return rec
}//- factory


const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
