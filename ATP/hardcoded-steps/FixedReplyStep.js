/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'


class FixedReplyStep extends Step {
  #returnError
  #note
  #reply

  constructor(definition) {
    super(definition)
    this.#returnError = definition.returnError ? true : false
    this.#note = definition.note ? definition.note : ''
    this.#reply = definition.reply ? definition.reply : { }
  }//- contructor

  async invoke(instance) {
    instance.trace(`FixedReplyStep (${instance.getStepId()})`)
    if (this.#returnError) {
      return await instance.failed(this.#note, this.#reply)
    } else {
      return await instance.succeeded(this.#note, this.#reply)
    }
  }//- invoke
}//- class FixedReplyStep

async function register() {
  await StepTypes.register(myDef, 'util/fixed-reply', 'Return a fixed reply')
}//- register

async function defaultDefinition() {
  return {
    returnError: false,
    note: 'Not supported',
    reply: {
      message: 'Operation not supported'
    },
  }
}
async function factory(definition) {
  const obj = new FixedReplyStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

// async function describe(definition) {
//   return {
//     stepType: definition.stepType,
//     description: definition.msg
//   }
// }

const myDef = {
  register,
  factory,
  // describe,
  defaultDefinition,
}
export default myDef
