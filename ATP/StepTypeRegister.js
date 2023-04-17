/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

export const STEP_TYPE_PIPELINE = 'hidden/pipeline'

const VERBOSE = 0

/**
 * This module maintains a list of step types.
 */
class StepTypeRegister {

  constructor() {
    this.index = [ ]
  }

  async register(stepModule, type, description = '') {
    if (VERBOSE) console.log(`- Registering step type ${type}`)
    if (!stepModule) {
      const msg = `StepTypeRegister.register(${type}) - stepModule is null`
      console.log(msg)
      throw new Error(msg)
    }
    if (typeof(type) !== 'string') {
      const msg = `StepTypeRegister.register(${type}) - stepModule is null`
      console.log(msg)
      throw new Error(msg)
    }
    if (!stepModule.factory || typeof(stepModule.factory) !== 'function') {
      console.log(stepModule)
      throw new Error(`StepTypeRegister.register(${type}) - invalid stepModule.factory`)
    }
    this.index[type] = {
      stepModule,
      description
    }
  }

  async getStepType(type) {
    const record = this.index[type]
    return record ? record.stepModule : null
  }

  async getDescription(type) {
    const record = this.index[type]
    return record ? record.description : ''
  }

  async factory(type, definition) {
    const stepModule = this.index[type]
    if (!stepModule) {
      // console.log(`this.index=`, this.index)
      throw new Error(`Unknown step type [${type}]`)
    }
    const step = await stepModule.stepModule.factory(definition)
    // console.log(`StepTypes.factory: step=`, step)
    return step
  }

  async names() {
    const list = [ ]
    for (const name in this.index) {
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

  async myStepTypes() {
    const list = [ ]
    for (const name in this.index) {
      const stepType = this.index[name]
      const record = {
        name,
        description: stepType.description,
      }
      if (stepType.stepModule.defaultDefinition) {
        record.defaultDefinition = await stepType.stepModule.defaultDefinition()
      } else {
        record.defaultDefinition = { }
      }
      if (!record.defaultDefinition.description) {
        record.defaultDefinition.description = stepType.description
      }
      list.push(record)
    }
    list.sort((a, b) => {
      if (a.name < b.name) return -1
      if (a.name > b.name) return +1
      return 0
    })
    return list
  }
}

const defaultRegister = new StepTypeRegister()
export default defaultRegister
