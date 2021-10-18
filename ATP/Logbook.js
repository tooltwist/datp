import dbLogbook from "../database/dbLogbook"

const LEVEL_TRACE = 'trace'
const LEVEL_WARNING = 'warning'
const LEVEL_ERROR = 'error'
const LEVEL_DEBUG = 'debug'

const VERBOSE = false

class Logbook {
  #transactionId
  // #pipelineId
  #sequencePrefix
  #sequence
  #autoPersist

  constructor(options) {
    if (VERBOSE) {
      console.log(`Logbook.constructor()`)
    }
    if (options) {
      if (VERBOSE) {
        console.log(`options:`, options)
      }

      // Check we have the required values
      if (!options.transactionId) {
        throw Error('Must specify options.transactionId')
      }
      // if (!options.pipelineId) {
      //   throw Error('Must specify options.pipelineId')
      // }

      // Remember the options
      this.#transactionId = options.transactionId
      // this.pipelineId = options.pipelineId
      if (options.sequencePrefix) {
        this.#sequencePrefix = options.sequencePrefix
      } else {
        this.#sequencePrefix = ''
      }
      if (options.autoPersist) {
        // Persist every log entry
        this.#autoPersist = true
      }
      this.#sequence = 0
      if (options.description) {
        this.description = options.description
      }
    }
    // ZZZZ We should identify the current process / server in log entries

    this._log = [ ]

    this.LEVEL_TRACE = LEVEL_TRACE
    this.LEVEL_WARNING = LEVEL_WARNING
    this.LEVEL_ERROR = LEVEL_ERROR
    this.LEVEL_DEBUG = LEVEL_DEBUG
  }

  async log(stepId, options, msg) {

    //ZZZ Only during development. We don't want transaction-specific log output in the console.
    // console.log(`stepId=`, stepId)
    // console.log(`options=`, options)
    // console.log(`msg=`, msg)
    // console.trace(`logbook: ${msg}`)

    // Save the logbook entry.
    const entry = {
      step: stepId,
      msg,
      ts: Date.now()
    }
    entry.isoTs = new Date(entry.ts).toISOString()
    let level = LEVEL_TRACE
    if (options) {
      if (options.level) {
        level = options.level
      }
      if (options.data) {
        entry.data = options.data
      }
      if (options.persist) {
        entry.persist = true
      }
    }
    entry.level = level

    // Save the log entry
    this._log.push(entry)

    // Perhaps persist the entry
    if (this.#autoPersist || entry.persist) {
      this.persist()
    }
  }

  /**
   * Write the log to persistent storage (e.g. database)
   */
  async persist() {
    console.log(`Logbook.persist`)
    dbLogbook.log()
  }

  // toString() {
  //   return `Logbook`
  // }
}

export default {
  cls: Logbook,
  LEVEL_TRACE,
  LEVEL_WARNING,
  LEVEL_ERROR,
  LEVEL_DEBUG,
}