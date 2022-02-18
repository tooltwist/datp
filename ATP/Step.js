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
        if (result.length < 1) {
          console.log(`Unknown pipeline [${pipelineName}]`)
          return null
        }
        const row = result[0]
        // console.log(`row=`, row)
        // console.log(`row.steps_json=`, row.steps_json)
        try {
          const steps = JSON.parse(row.steps_json)
          definition = {
            stepType: STEP_TYPE_PIPELINE,
            name: row.name,
            description: row.description,
            notes: row.notes,
            steps,
          }
          // console.log(`PIPELINE definition=`, definition)
        } catch (e) {
          console.log(`Error parsing definition of pipeline ${definition}:`, e)
        }

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