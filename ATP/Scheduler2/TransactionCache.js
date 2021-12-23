import query from '../../database/query'
import GenerateHash from '../GenerateHash'
import XData from '../XData'
import Transaction from './Transaction'
import TransactionPersistance from './TransactionPersistance'
import assert from 'assert'
class TransactionCache {
  #cache

  constructor() {
    this.#cache = [ ] // txId => Transaction2
  }

  /**
   *
   * @param {XData} input Input data for the new transaction.
   * @returns
   */
  async newTransaction(owner, externalId, transactionType) {
    // console.log(`TransactionCache.newTransaction(${owner}, ${externalId}, ${transactionType})`)

    assert(owner)
    assert(transactionType)

    // Create the initial transaction
    const txId = GenerateHash('tx')
    const tx = new Transaction(txId, owner, externalId, transactionType)
    this.#cache[txId] = tx

    // Persist this transaction
    await TransactionPersistance.saveNewTransaction(tx)
    return tx
  }

  /**
   *
   * @param {string} txId
   * @param {boolean} loadIfNecessary If true, load the memory from persistant
   * storage, if it is there
   * @returns {Promise<Transaction>} A Transaction if it is found, or null if it is not in the cache,
   * and is also not in persistant storage if loadIfNecessary is true.
   */
  async findTransaction(txId, loadIfNecessary = false) {
    assert(typeof(txId) === 'string')
    assert(typeof(loadIfNecessary) === 'boolean')

    let tx = this.#cache[txId]
    if (tx) {
      return tx
    } else if (loadIfNecessary) {
      // Try loading the transaction from persistant storage
      tx = await TransactionPersistance.reconstructTransaction(txId)
      if (tx) {
        this.#cache[txId] = tx
        return tx
      }
    }
    return null
  }

  /**
   *
   * @param {string} txId
   * @param {boolean} loadIfNecessary If true, load the memory from persistant
   * storage, if it is there
   * @returns {Promise<Transaction>} A Transaction if it is found, or null if it is not in the cache,
   * and is also not in persistant storage if loadIfNecessary is true.
   */
  async findTransactionByExternalId(owner, externalId, loadIfNecessary = false) {
    assert(typeof(owner) === 'string')
    assert(typeof(externalId) === 'string')
    assert(typeof(loadIfNecessary) === 'boolean')

    const sql = `SELECT transaction_id FROM atp_transaction2 WHERE owner=? AND external_id=?`
    const params = [ owner, externalId ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)
    if (rows < 1) {
      return null
    }

    return this.findTransaction(rows[0].transaction_id, loadIfNecessary)
  }

  /**
   *
   * @param {string} txId
   */
  async removeFromCache(txId) {
    delete this.#cache[txId]
  }

  /**
   *
   * @param {string} txId
   * @param {boolean} removeFromCache
   */
  async persist(txId, removeFromCache = true) {
    // console.log(`TransactionCache.persist(${txId})`)

    const tx = this.#cache[txId]
    if (tx) {
      // console.log(`tx=`, tx)
      //ZZZZ Handle errors carefully here YARP2
      // console.log(`persist the transaction`)
      await TransactionPersistance.persistDeltas(tx)

      if (removeFromCache) {
        // console.log(`removing the transaction from the cache`)
        delete this.#cache[txId]
      }
    }
  }

  /**
   *
   */
  async dump() {
    console.log(`Transaction cache:`)
    for (let txId in this.#cache) {
      const tx = this.#cache[txId]
      console.log(`  ${txId}, ${tx.toString()}`)
    }
  }
}

export default TransactionCache = new TransactionCache()