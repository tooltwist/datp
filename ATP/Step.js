/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import StepTypeRegister from './StepTypeRegister'
import query from '../database/query'
import { STEP_TYPE_PIPELINE } from './StepTypeRegister'

const VERBOSE = false

/**
 * Step is queued for running
 */
export const STEP_QUEUED = 'queued'

/**
 * Step is currently running
 */
export const STEP_RUNNING = 'running'
// static ERROR = 'error'

/**
 * Step completed without error.
 */
export const STEP_SUCCESS = 'success'

/**
 * Pipeline step failed - attempt rollback.
 */
export const STEP_FAILED = 'failed'

/**
 * Do not try to rollback.
 */
export const STEP_ABORTED = 'aborted'

/**
 * Step is sleeping, until either a certain time, of an external "nudge".
 */
export const STEP_SLEEPING = 'sleeping'

/**
 * The step ran for more than the permitted amount of time.
 */
export const STEP_TIMEOUT = 'timeout'

/**
 * Error related to pipeline definition.
 */
export const STEP_INTERNAL_ERROR = 'internal-error'

  // static TERMINATED,
  // static ERROR,
  // static OFFLINE,

export default class Step {



  constructor(definition) {
    // this.id = GenerateHash('step')
    if (VERBOSE) {
      this.definition = definition
    }

  }//- constructor

  async invoke_internal(instance) {
    // console.log(`\n\n\n\n\n INVOKE STEP \n${new Error('YARP').stack}\n\n\n\n`)
    const hackSource = 'system' // Not sure why, but using Transaction.LOG_SOURCE_SYSTEM causes a compile error
    instance.trace(`Starting as step ${instance.getStepId()}`, hackSource)
    await instance.syncLogs()

    // Start the step in the background, immediately
    setTimeout(async () => {
      try {
        await this.invoke(instance) // Provided by the step implementation
      } catch (e) {
// console.log(`\n\n\nException in Step:`, e)
//         instance.console(``)
//         // console.log(`Exception occurred while running step ${instance.getStepId()}:`)
//         instance.console(`Exception occurred while running step ${instance.getStepId()}:`)
//         instance.console(``)
//         //ZZZZZ
//         // handle the error better
//         console.log(e)
//         instance.console(``)
//         instance.console(``)

        return await instance.exceptionInStep(null, e)
        // const note = `Exception in step.invoke()`
        // const data = {
        //   error: `Exception`
        // }
        // instance.finish(STEP_INTERNAL_ERROR, note, data)
      }
    }, 0)
      // let reply = this.invoke(instance) // Provided by the step implementation
      // if (reply) {
      //   return reply
      // }

    return {
      stepId: instance.getStepId(),
      status: STEP_SLEEPING,
    }
  }

  static async resolveDefinition(definition) {
    /*
      *  Load the definition of the step (which is probably a pipeline)
      */
    // console.log(`typeof(options.definition)=`, typeof(options.definition))
    switch (typeof(definition)) {
      case 'string':
        // console.log(`Loading definition for pipeline ${definition}`)
        // const rawdata = fs.readFileSync(`./pipeline-definitions/transaction-${definition}.json`)
        const pipelineName = definition
        const sql = `SELECT node_name, name, version, description, steps_json, notes FROM atp_pipeline WHERE name=?`
        const params = [ pipelineName ]
        const result = await query(sql, params)

        const row = result[0]
        // console.log(`row=`, row)
        // console.log(`row.steps_json=`, row.steps_json)
        const steps = JSON.parse(row.steps_json)
        definition = {
          stepType: STEP_TYPE_PIPELINE,
          name: row.name,
          description: row.description,
          notes: row.notes,
          steps,
        }
        // console.log(`PIPELINE definition=`, definition)

    case 'object':
      // console.log(`already have definition`)
      break

    default:
      throw new Error(`Invalid value for parameter definition (${typeof(definition)})`)
    }
    return definition
  }


  static async describe(definition) {
    console.log(`stepDescription()`)
    try {
      definition = await Step.resolveDefinition(definition)
    } catch (e) {
      console.log(`Error resolving pipeline definition (${definition}):`, e)
    }
    console.log(`definition=`, definition)
    const stepTypeObject = await StepTypeRegister.getStepType(definition.stepType)
    console.log(`stepTypeObject=`, stepTypeObject)


    if (stepTypeObject.describe) {
      // This step type wants to describe itself
      const description = await stepTypeObject.describe(definition)
      console.log(`description=`, description)
      return description
    } else {
      // Default description
      console.log(`default step description`)
      return {
        stepType: definition.stepType
      }
    }
  }//- stepDescription

}