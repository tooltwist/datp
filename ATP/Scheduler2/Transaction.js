/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from "../../database/query"
import { deepCopy } from "../../lib/deepCopy"
import {
  STEP_RUNNING,
  STEP_QUEUED,
  STEP_SUCCESS,
  STEP_FAILED,
  STEP_ABORTED,
  STEP_SLEEPING,
  STEP_TIMEOUT,
  STEP_INTERNAL_ERROR
} from "../Step"
import XData from "../XData"
import TransactionPersistance from "./TransactionPersistance"
import Scheduler2 from "./Scheduler2"
import { schedulerForThisNode } from "../.."

// Debug stuff
const VERBOSE = 0
require('colors')

export default class Transaction {
  #txId
  #owner
  #externalId
  #transactionType
  #status
  #sequenceOfUpdate
  #progressReport
  #transactionOutput
  #completionTime

  // Sleep related stuff
  #sleepingSince
  #sleepCounter
  #wakeTime
  #wakeSwitch
  #wakeNodeGroup
  #wakeStepId

  #steps // stepId => Object
  #tx // Object

  #deltaCounter
  #deltas

  // We use this as a check that the user is not trying to process
  // a delta before the previous delta has been completed.
  #processingDelta

  /**
   *
   * @param {String} txId Transaction ID
   * @param {XData} definition Mandatory, immutable parameters for a transaction
   */
  constructor(txId, owner, externalId, transactionType) {
    // console.log(`Transaction.constructor(${txId}, ${owner}, ${externalId}, ${transactionType})`)

    // Check the parameters
    if (typeof(txId) !== 'string') { throw new Error(`Invalid parameter [txId]`) }
    if (typeof(owner) !== 'string') { throw new Error(`Invalid parameter [owner]`) }
    if (externalId!==null && typeof(externalId) !== 'string') { throw new Error(`Invalid parameter [externalId]`) }
    if (typeof(transactionType) !== 'string') { throw new Error(`Invalid parameter [transactionType]`) }

    // Core transaction information
    this.#txId = txId
    this.#owner = owner
    this.#externalId = externalId
    this.#transactionType = transactionType
    this.#status = STEP_RUNNING
    this.#sequenceOfUpdate = 0
    this.#progressReport = {}
    this.#transactionOutput = {}
    this.#completionTime = null

    // Sleep-related fields
    this.#sleepCounter = 0
    this.#sleepingSince = null
    this.#wakeTime = null
    this.#wakeSwitch = null
    this.#wakeNodeGroup = null
    this.#wakeStepId = null

    // Initialise the places where we store transaction and step data
    this.#tx = {
      status: STEP_RUNNING
    }
    this.#steps = { } // stepId => { }

    this.#deltaCounter = 0
    this.#deltas = [ ]

    this.#processingDelta = false
  }

  getTxId() {
    return this.#txId
  }

  getExternalId() {
    return this.#externalId
  }

  getOwner() {
    return this.#owner
  }

  getTransactionType() {
    return this.#transactionType
  }

  getStatus() {
    return this.#status
  }

  getSequenceOfUpdate() {
    return this.#sequenceOfUpdate
  }

  getDeltaCounter() {
    return this.#deltaCounter
  }

  getProgressReport() {
    return this.#progressReport
  }

  getTransactionOutput() {
    return this.#transactionOutput
  }

  getCompletionTime() {
    return this.#completionTime
  }

  asObject() {
    return {
      txId: this.#txId,
      owner: this.#owner,
      externalId: this.#externalId,
      transactionData: this.#tx,
      steps: this.#steps
    }
  }

  txData() {
    return this.#tx
  }

  transactionData() {
    return this.#tx
  }

  stepData(stepId) {
    const d = this.#steps[stepId]
    if (d) {
      return d
    }
    return null
  }

  /**
   * Record data changes to this transaction or a step within this transaction.
   *
   * When the following fields are set on the trnsaction, they also get persisted to the
   * atp_transaction2 table:
   * status, progressReport, transactionOutput, completionTime.
   *
   * @param {string} stepId If null, the data changes will be saved against the transaction.
   * @param {object} data An object containing values to be saved. e.g. { color: 'red' }
   * @param {boolean} replayingPastDeltas Set to true only when reloading from database (do not use this).
   */
  async delta(stepId, data, note='', replayingPastDeltas=false) {
    let id = stepId ? stepId.substring(2, 10) : 'tx'
    // console.log(`delta - ${note} - ${id}`)
    if (VERBOSE) console.log(`\n*** delta(${stepId}, data, replayingPastDeltas=${replayingPastDeltas})`, data)

    // Check that this function is not already in action, because someone forgot an 'await'.
    if (this.#processingDelta) {
      //ZZZZ Raise an alarm
      console.log(`VERY SERIOUS ERROR: delta() was called again before it completed. Missing 'await'?`)
      throw new Error(`delta() was called again before it completed. Missing 'await'?`)
    }
    this.#processingDelta = true
    try {

      // Next sequence number
      this.#deltaCounter++

      // See if any of the core transaction values are changing
      let coreValuesChanged = false
      if (stepId === null) {
        if (data.status) {
          // Check the status is valid
          switch (data.status) {
            // These are temporary ststuses that do not impact sleeping values
            case STEP_QUEUED:
            case STEP_RUNNING:
              if (this.#status !== data.status) {
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#status = data.status
              }
              break

            // These statuses need to reset the sleeping values
            case STEP_SUCCESS:
            case STEP_FAILED:
            case STEP_ABORTED:
            case STEP_TIMEOUT:
            case STEP_INTERNAL_ERROR:
              if (this.#status !== data.status) {
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#status = data.status
              }
              // We are no longer in a sleep loop
              if (this.#sleepCounter!==0 || this.#sleepingSince!==null || this.#wakeTime!==null || this.#wakeSwitch!==null) {
                coreValuesChanged = true
                this.#sleepCounter = 0
                this.#sleepingSince = null
                this.#wakeTime = null
                this.#wakeSwitch = null
                this.#wakeNodeGroup = null
                this.#wakeStepId = null
                if (VERBOSE) console.log(`Resetting sleep values`)
              }
              break

            case STEP_SLEEPING:
              if (this.#status !== data.status) {
                // Was not already in sleep mode
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#status = data.status
                if (this.#sleepingSince === null) {
                  if (VERBOSE) console.log(`Initializing sleep fields`)
                  this.#sleepCounter = 1
                  this.#sleepingSince = new Date()
                  this.#wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#wakeStepId = data.wakeStepId
                } else {
                  if (VERBOSE) console.log(`Incrementing sleep counter`)
                  this.#sleepCounter++
                }
              }
              // if (typeof(data.wakeTime) !== 'undefined' && this.#wakeTime !== data.wakeTime) {
              //   if (VERBOSE) console.log(`Setting wakeTime to ${data.wakeTime}`)
              //   if (data.wakeTime === null || data.wakeTime instanceof Date) {
              //     coreValuesChanged = true
              //     this.#wakeTime = data.wakeTime
              //   } else {
              //     throw new Error('Invalid data.wakeTime')
              //   }
              // }
              if (typeof(data.sleepDuration) !== 'undefined') {
                let newWakeTime
                if (data.sleepDuration === null) {
                  // All good
                  newWakeTime = null
                } else if (typeof(data.sleepDuration) === 'number') {
                  // This value is used by the scheduler to restart steps. We'll add on fifteen
                  // seconds for the scheduler, so that if we have a setTimeut set to trigger
                  // a short timeout, it gets in before the scheduler.
                  newWakeTime = new Date(Date.now() + (data.sleepDuration * 1000) + (15 * 1000))
                } else {
                  throw new Error('Invalid data.sleepDuration')
                }
                if (this.#wakeTime !== newWakeTime) {
                  if (VERBOSE) console.log(`Setting wakeTime to +${newWakeTime}`)
                  coreValuesChanged = true
                  this.#wakeTime = newWakeTime
                  this.#wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#wakeStepId = data.wakeStepId
                }
              }
              if (typeof(data.wakeSwitch) !== 'undefined' && this.#wakeSwitch !== data.wakeSwitch) {
                if (VERBOSE) console.log(`Setting wakeSwitch to ${data.wakeSwitch}`)
                if (data.wakeSwitch === null || typeof(data.wakeSwitch) === 'string') {
                  coreValuesChanged = true
                  this.#wakeSwitch = data.wakeSwitch
                  this.#wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#wakeStepId = data.wakeStepId
                } else {
                  throw new Error('Invalid data.wakeSwitch')
                }
              }
              break

            default:
              throw new Error(`Invalid status [${data.status}]`)
          }
        }
        if (typeof(data.progressReport) !== 'undefined' && this.#progressReport !== data.progressReport) {
          if (VERBOSE) console.log(`Setting transaction progressReport to ${data.progressReport}`)
          if (typeof(data.progressReport) !== 'object') {
            throw new Error('data.progressReport must be an object')
          }
          coreValuesChanged = true
          this.#progressReport = data.progressReport
        }
        if (typeof(data.transactionOutput) !== 'undefined' && this.#transactionOutput !== data.transactionOutput) {
          if (VERBOSE) console.log(`Setting transactionOutput to ${data.transactionOutput}`)
          if (typeof(data.transactionOutput) !== 'object') {
            throw new Error('data.transactionOutput must be an object')
          }
          coreValuesChanged = true
          this.#transactionOutput = data.transactionOutput
        }
        if (typeof(data.completionTime) !== 'undefined' && this.#completionTime !== data.completionTime) {
          if (VERBOSE) console.log(`Setting transaction completionTime to ${data.completionTime}`)
          if (data.completionTime !== null && !(data.completionTime instanceof Date)) {
            throw new Error('data.completionTime parameter must be of type Date')
          }
          coreValuesChanged = true
          this.#completionTime = data.completionTime
        }

                    // if (this.#status === STEP_SLEEPING) {
            //   if (this.)

            // } else if (this.#status === STEP_ || this.#status === STEP_SLEEPING)


        // If the core values were changed, update the database
        if (coreValuesChanged) {
          this.#sequenceOfUpdate = this.#deltaCounter
          if (!replayingPastDeltas) {
            let sql = `UPDATE atp_transaction2 SET
              status=?,
              progress_report=?,
              transaction_output=?,
              completion_time=?,
              sequence_of_update=?,
              sleep_counter=?,
              sleeping_since=?,
              wake_time=?,
              wake_switch=?,
              wake_node_group=?,
              wake_step_id=?`

            //ZZZZ Check that the transaction hasn't been updated by someone else.
            //  AND sequence_of_update=?   [ this.#sequenceOfUpdate ]
            const transactionOutputJSON = this.#transactionOutput ? JSON.stringify(this.#transactionOutput) : null
            const progressReportJSON = this.#progressReport ? JSON.stringify(this.#progressReport) : null
            const params = [
              this.#status,
              progressReportJSON,
              transactionOutputJSON,
              this.#completionTime,
              this.#deltaCounter,
              this.#sleepCounter,
              this.#sleepingSince,
              this.#wakeTime,
              this.#wakeSwitch,
              this.#wakeNodeGroup,
              this.#wakeStepId
            ]
            sql += ` WHERE transaction_id=? AND owner=?`
            params.push(this.#txId)
            params.push(this.#owner)
            // console.log(`sql=`, sql)
            // console.log(`params=`, params)
            const response = await query(sql, params)
            // console.log(`response=`, response)
            if (response.changedRows !== 1) {
              // This should never happen.
              //ZZZZZ What do we do if the transaction was not updated
              throw new Error(`INTERNAL ERROR: Could not update transaction record`)
            }

            // Notify any event handler
            // console.log(`this.#tx=`, this.#tx)
            if (this.#tx.onChange) {
              const queueName = Scheduler2.groupQueueName(this.#tx.nodeGroup)
              if (VERBOSE) console.log(`Adding a TRANSACTION_CHANGE_EVENT to queue ${queueName}`)
              await schedulerForThisNode.enqueue_TransactionChange(queueName, {
                owner: this.#owner,
                txId: this.#txId
              })
            }
          }//- !replayingPastDeltas
        }//- coreValuesChanged
      }//- !stepId

      // Save this delta (i.e. like a journal entry)
      const obj = {
        sequence: this.#deltaCounter,
        stepId,
        data,
        time: new Date()
      }
      this.#deltas.push(obj)
      if (!replayingPastDeltas) {
        // Save the delta to the database
        await TransactionPersistance.persistDelta(this.#owner, this.#txId, obj)
      }

      // Update this in-memory transaction object
      if (stepId) {
        // We are updating a step
        let step = this.#steps[stepId]
        if (step === undefined) {
          step = { }
          this.#steps[stepId] = step
        }
        deepCopy(data, step)
      } else {
        // We are updating the transaction
        deepCopy(data, this.#tx)
      }
      this.#processingDelta = false
    } catch (e) {
      this.#processingDelta = false
      throw e
    }
  }//- delta


  static async getSummary(owner, txId) {
    const sql = `SELECT
      owner,
      transaction_id AS txId,
      external_id AS externalId,
      status,
      sequence_of_update AS sequenceOfUpdate,
      progress_report AS progressReport,
      transaction_output AS transactionOutput,
      completion_time AS completionTime,
      last_updated AS lastUpdated
      FROM atp_transaction2
      WHERE owner=? AND transaction_id=?`
    const params = [ owner, txId ]
    const rows = await query(sql, params)
    if (rows.length < 1) {
      return null
    }
    const row = rows[0]
    const transactionOutput = row.transactionOutput
    const progressReport = row.progressReport
    const summary = {
      metadata: row,
      progressReport
    }
    delete summary.metadata.transactionOutput
    delete summary.metadata.progressReport
    try { summary.progressReport = JSON.parse(progressReport) } catch (e) { /* We'll stick with the JSON string */ }
    if (
      row.status === STEP_SUCCESS
      || row.status === STEP_FAILED
      || row.status === STEP_ABORTED
      || row.status === STEP_INTERNAL_ERROR
      || row.status === STEP_TIMEOUT
    ) {
      // Step has completed
      try {
        if (!transactionOutput) transactionOutput = '{ }'
        // console.log(`transactionOutput=`, transactionOutput)
        summary.data = JSON.parse(transactionOutput)
        // console.log(`summary.data=`, summary.data)
      } catch (e) {
        summary.data = transactionOutput
      }
    } else {
      // Step is still in progress
      // try {
      //   if (!progressReport) progressReport = '{ }'
      //   summary.progressReport = JSON.parse(progressReport)
      // } catch (e) {
      //   summary.progressReport = progressReport
      // }
    }
    // console.log(`summary=`, summary)

    return summary
  }


  static async getSummaryByExternalId(owner, externalId) {
    const sql = `SELECT
      owner,
      transaction_id AS txId,
      external_id AS externalId,
      status,
      sequence_of_update AS sequenceOfUpdate,
      progress_report AS progressReport,
      transaction_output AS transactionOutput,
      completion_time AS completionTime,
      last_updated AS lastUpdated
      FROM atp_transaction2
      WHERE owner=? AND external_id=?`
    const params = [ owner, externalId ]
    const rows = await query(sql, params)
    if (rows.length < 1) {
      return null
    }
    const row = rows[0]
    const transactionOutput = row.transactionOutput
    const progressReport = row.progressReport
    const summary = {
      metadata: row,
      progressReport
    }
    delete summary.metadata.transactionOutput
    delete summary.metadata.progressReport
    try { summary.progressReport = JSON.parse(progressReport) } catch (e) { /* We'll stick with the JSON string */ }
    if (
      row.status === STEP_SUCCESS
      || row.status === STEP_FAILED
      || row.status === STEP_ABORTED
      || row.status === STEP_INTERNAL_ERROR
      || row.status === STEP_TIMEOUT
    ) {
      // Step has completed
      try {
        if (!transactionOutput) transactionOutput = '{ }'
        // console.log(`transactionOutput=`, transactionOutput)
        summary.data = JSON.parse(transactionOutput)
        // console.log(`summary.data=`, summary.data)
      } catch (e) {
        summary.data = transactionOutput
      }
    } else {
      // Step is still in progress
      // try {
      //   if (!progressReport) progressReport = '{ }'
      //   summary.progressReport = JSON.parse(progressReport)
      // } catch (e) {
      //   summary.progressReport = progressReport
      // }
    }
    // console.log(`summary=`, summary)

    return summary
  }


  static async getSwitches(owner, txId) {
    const sql = `SELECT
      sequence_of_update,
      switches
      FROM atp_transaction2
      WHERE owner=? AND transaction_id=?`
    const params = [ owner, txId ]
    const rows = await query(sql, params)
    if (rows.length < 1) {
      return [ ]
    }

    let switches = { }
    try {
      if (rows[0].switches) {
        switches = JSON.parse(rows[0].switches)
      }
    } catch (e) {
      //ZZZZZ report alert
      const description = `Corrupt switches for transaction ${owner}, ${txId}`
      console.log(description)
      throw new Error(description)
    }
    return { sequenceOfUpdate: rows[0].sequence_of_update, switches }
  }

  static async getSwitch(owner, txId, name) {
    const { switches } = await Transaction.getSwitches(owner, txId)
    if (typeof(switches[name]) === 'undefined') {
      return null
    }
    return switches[name]
  }

  /**
   * Set a transaction switch value
   * @param {String} owner
   * @param {string} txId
   * @param {string} name
   * @param {string|number|bookean} value If the value is null, the switch will be removed
   * @param {boolean} triggerNudgeEvent If true, a NUDGE_EVENT will be triggered for the transaction
   * if we successfully change the switch value. If the transaction is currently sleeping, this will
   * cause the current sleeping step to be immediately retried.
   */
  static async setSwitch(owner, txId, name, value, triggerNudgeEvent=false) {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(owner, txId)
    if (value === null || typeof(value) === 'undefined') {

      // Remove the switch
      delete switches[name]
    } else {

      // Add the switch
      switch (typeof(value)) {
        case 'string':
          if (value.length > 32) {
            throw new Error(`Switch exceeds maximum length (${name}: ${value})`)
          }
          switches[name] = value
          break

        case 'boolean':
        case 'number':
          switches[name] = value
          break

        default:
          throw new Error(`Switch can only be boolean, number, or string (< 32 chars)`)
      }
    }

    const json = JSON.stringify(switches, '', 0)

    // Update the transaction, ensuring nobody has changed the transaction before us
    const sql = `UPDATE atp_transaction2 SET switches=?, sequence_of_update=? WHERE owner=? AND transaction_id=? AND sequence_of_update=?`
    const params = [ json, sequenceOfUpdate+1, owner, txId, sequenceOfUpdate ]
    const reply = await query(sql, params)
    // console.log(`reply=`, reply)
    if (reply.changedRows !== 1) {
      throw new Error('Concurrent attempts to set transaction switches - switch not set')
    }

    // Trigger an event for this change?
    if (triggerNudgeEvent) {
      console.log(`YARP Event not triggered`)
    }
  }

  /**
   *
   * @returns
   */
  getSleepingSince() {
    return this.#sleepingSince
  }

  /**
   *
   * @returns
   */
  getRetryCounter() {
    return this.#sleepCounter
  }

  /**
   *
   * @returns
   */
  getWakeTime() {
    return this.#wakeTime
  }

  /**
   *
   * @returns
   */
  getWakeSwitch() {
    return this.#wakeSwitch
  }

  getWakeNodeGroup() {
    return this.#wakeNodeGroup
  }

  getWakeStepId() {
    return this.#wakeStepId
  }

  /**
   *
   * @param {number} pagesize
   * @param {number} offset
   * @param {string} filter
   * @param {string[]} status
   * @returns
   */
  static async findTransactions(pagesize=20, offset=0, filter='', statusList=[STEP_SUCCESS, STEP_FAILED, STEP_ABORTED]) {
    // console.log(`findTransactions()`, options)

    let sql = `SELECT
      transaction_id AS txId,
      owner,
      external_id AS externalId,
      transaction_type AS transactionType,
      status,
      start_time AS startTime,
      completion_time AS completionTime,
      last_updated AS lastUpdated,
      response_acknowledge_time AS responseAcknowledgeTime,
      switches,
      sleep_counter AS sleepCounter,
      sleeping_since AS sleepingSince,
      wake_time AS wakeTime,
      wake_switch AS wakeSwitch
      FROM atp_transaction2`
    const params = [ ]

    let sep = `\nWHERE`
    // if (options.sleeping) {
    //   sql += ` ${sep} sleep_counter > 0`
    //   sep = `AND`
    // }
    // if (options.finished) {
    //   sql += ` ${sep} (status='${STEP_SUCCESS}' OR status='${STEP_FAILED}' OR status='${STEP_ABORTED}')`
    //   sep = `AND`
    // }

    if (statusList) {
      sql += `${sep} (`
      let sep2 = ''
      for (const s of statusList) {
        if (s) {
          sql += `${sep2} status=?`
          params.push(s)
          sep2 = ' OR '
        }
      }
      sql += `)`
      sep = `\nAND `
    }
    if (filter) {
      sql += `${sep} (
        transaction_id LIKE ?
        OR owner LIKE ?
        OR external_id LIKE ?
        OR transaction_type LIKE ?
        OR wake_switch =?
      )`
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
    }

    // pagination
    sql += `\nORDER BY last_updated DESC LIMIT ? OFFSET ?`
    // 2022-03-15 Phil - need to pass parameters as strings due to MySQL bug.
    // See https://stackoverflow.com/questions/65543753/error-incorrect-arguments-to-mysqld-stmt-execute
    params.push('' + pagesize)
    params.push('' + offset)

    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const rows = await query(sql, params)
    // console.log(`${rows.length} transactions.`)
    return rows
  }

  stepIds() {
    // console.log(`stepIds()`)
    const stepIds = Object.keys(this.#steps)
    // console.log(`stepIds=`, stepIds)
    return stepIds
  }


  async getDetails(withSteps=true, withDeltas=false) {
    const obj = {
      txId: this.#txId,
      owner: this.#owner,
      externalId: this.#externalId,
      status: this.#status,
      sequenceOfUpdate: this.#sequenceOfUpdate,
      progressReport: this.#progressReport,
      transactionOutput: this.#transactionOutput,
      completionTime: this.#completionTime
    }
    if (withSteps) {
      obj.steps = [ ]
      for (const stepId in this.#steps) {
      // for (const step of this.#steps) {
        // console.log(`stepId=`, stepId)
        const step = this.#steps[stepId]
        // console.log(`step=`, step)
        const stepObj = {
          stepId,
          parentStepId: step.parentStepId,
          status: step.status,
          stepInput: step.stepInput,
          stepOutput: step.stepOutput,
          level: step.level,
          note: step.note,
        }
        stepObj.stepDefinition = step.stepDefinition
        if (typeof (step.stepDefinition) === 'string') {
          stepObj.isPipeline = true
          stepObj.pipelineName = step.stepDefinition
          stepObj.children = [ ]
          // console.log(`step.children.length=`, step.childStepIds.length)
          for (const childStepId of step.childStepIds) {
            // console.log(`childStepId=`, childStepId)
            stepObj.children.push(childStepId)
          }
        } else {
          stepObj.isPipeline = false
        }
        obj.steps.push(stepObj)
      }
    }
    return obj
  }


  getDeltas() {
    const deltas = this.#deltas
    this.#deltas = [ ]
    return deltas
  }

  toString() {
    return JSON.stringify(this.asObject())
  }
}
