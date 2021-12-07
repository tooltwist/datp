import TransactionIndexEntry from "../TransactionIndexEntry"
import TxData from "../TxData"
// import TransactionPersistance from './TransactionPersistance'

export default class Transaction {
  #txId
  #owner
  #externalId

  #steps // stepId => Object
  #tx // Object

  #deltaCounter
  #deltas

  /**
   *
   * @param {String} txId Transaction ID
   * @param {TxData} definition Mandatory, immutable parameters for a transaction
   */
  constructor(txId, owner, externalId) {
    // Check the parameters
    if (typeof(txId) !== 'string') { throw new Error(`Invalid parameter [txId]`) }
    if (typeof(owner) !== 'string') { throw new Error(`Invalid parameter [owner]`) }
    if (externalId!==null && typeof(externalId) !== 'string') { throw new Error(`Invalid parameter [externalId]`) }

    this.#txId = txId
    this.#owner = owner
    this.#externalId = externalId

    // Initialise the places where we store transaction and step data
    this.#tx = {
      status: TransactionIndexEntry.RUNNING //ZZZZZ YARP2
    }
    this.#steps = { } // stepId => { }

    this.#deltaCounter = 1
    this.#deltas = [ ]
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

  getStatus() {
    return this.#tx.status
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

  delta(stepId, data) {
    // console.log(`delta(${stepId})`, data)

    this.#deltas.push({
      sequence: this.#deltaCounter++,
      stepId,
      data: JSON.stringify(data),
      time: new Date()
    })

    if (stepId) {
      // We are updating a step
      let step = this.#steps[stepId]
      if (step === undefined) {
        step = { }
        this.#steps[stepId] = step
      }
      cloneData(data, step)
    } else {
      // We are updating the transaction
      cloneData(data, this.#tx)
    }
  }//- delta

  getDeltas() {
    const deltas = this.#deltas
    this.#deltas = [ ]
    return deltas
  }

  toString() {
    return JSON.stringify(this.asObject())
  }
}

function cloneData(from, to) {
  // console.log(`cloneData()   ${JSON.stringify(from)}  =>  ${JSON.stringify(to)}`)
  for (let name in from) {
    const value = from[name]

    // Perhaps delete the value?
    if (name.startsWith('-')) {
      name = name.substring(1)
      delete to[name]
      continue
    }

    // Nope, setting the value
    // console.log(`-> ${name}=${value}`)
    const type = typeof(value)
    switch (type) {
      case 'string':
      case 'number':
        to[name] = value
        break
      case 'object':
        let nested = to[name]
        if (!nested) {
          nested = { }
          to[name] = nested
        }
        cloneData(value, nested)
        break
      default:
        console.log(`cloneData: Unknown type [${type}] for ${name}`)
        throw new Error(`Transaction.cloneData(): unknown data type ${type}`)
    }
  }
}
