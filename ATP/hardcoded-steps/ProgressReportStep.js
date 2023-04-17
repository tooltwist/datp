/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'
import StepInstance from "../StepInstance"

const VERBOSE = 0

class ProgressReportStep extends Step {
  #progressReport

  constructor(definition) {
    super(definition)

    this.#progressReport = { message: 'Lorem Ipsum...' }

    if (definition.progressReport) {
      this.#progressReport = definition.progressReport
    }
  }//- constructor

  /**
   * 
   * @param {StepInstance} instance 
   * @returns void
   */
  async invoke(instance) {
    if (VERBOSE) {
      console.log(`ProgressReportStep (${instance.getStepId()})`)
    }

    // Update the progress report
    await instance.progressReport(this.#progressReport)

    // All good, carry on to the next step
    return await instance.succeeded('Progress report updated')
  }//- invoke
}//- class ProgressReportStep

async function register() {
  await StepTypes.register(myDef, 'util/progressReport', 'Provide a progress report for the API client')
}//- register

async function defaultDefinition() {
  return {
    description: 'Report progress',
    progressReport: { message: 'Lorem Ipsum...' }
  }
}

async function factory(definition) {
  const obj = new ProgressReportStep(definition)
  return obj
}//- factory

const myDef = {
  register,
  factory,
  defaultDefinition,
}
export default myDef
