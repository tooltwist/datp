/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'
import query from '../../database/query'

const VERBOSE = true

class MockStep extends Step {

  constructor(definition) {
    super(definition)
    this.msg = definition.msg
  }//- contructor

  async invoke(instance) {
    if (VERBOSE) {
      // instance.console(`*****`)
      instance.console(`MockStep (${instance.getStepId()})`)
      instance.console(`"${this.msg}"`)
      // instance.console(`*****`)
      // console.log(`this=`, this)
      // console.log(`instance=`, instance)
    }
    const data = instance.getDataAsObject()

    instance.privateData.dummy = 'Dummy was here!'

    // logbook.log(this.stepId, `MockStep.invoke()`, {
    //   level: logbook.LEVEL_DEBUG,
    //   data: instance.data,
    // })
    const reply = await query(`SELECT * from map_service`)
    // console.log(`++++++++++++++++++++++++++++++++++++++++++ reply=`, reply)


    // setTimeout(() => {
      data.said = 'nothing'
      const note = this.msg
      instance.finish(Step.COMPLETED, note, data)
    // }, 1000)
  }//- invoke

  // async getNote() {
  //   return 'NoNoye'
  // }
}//- class Dummy

async function register() {
  await StepTypes.register(myDef, 'mock', 'Simulation step')
}//- register

async function defaultDefinition() {
  return {
    value1: 'Hello World',
    value2: 12345,
  }
}
async function factory(definition) {
  const obj = new MockStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

async function describe(definition) {
  return {
    stepType: definition.stepType,
    description: definition.msg
  }
}

const myDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default myDef
