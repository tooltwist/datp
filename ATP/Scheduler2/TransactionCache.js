/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from '../../database/query'
import GenerateHash from '../GenerateHash'
import XData from '../XData'
import Transaction from './Transaction'
import TransactionPersistance from './TransactionPersistance'
import assert from 'assert'
import { schedulerForThisNode } from '../..'
import { objectsAreTheSame } from '../../lib/objectDiff'

const PERSIST_FAST_DURATION = 10 // Almost immediately
const PERSIST_REGULAR_DURATION = 120 // Two minutes

const VERBOSE = 0

class TransactionCache {
  #cache

  constructor() {
    this.#cache = new Map() // txId => Transaction2
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
    this.#cache.set(txId, tx)

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
  async getTransactionState(txId, loadIfNecessary = true, saveInLocalMemoryCache = true) {
    assert(typeof(txId) === 'string')
    assert(typeof(loadIfNecessary) === 'boolean')
    assert(typeof(saveInLocalMemoryCache) === 'boolean')

    let tx = this.#cache.get(txId)
    if (tx) {

      // Already in our in-memory cache
      if (VERBOSE) console.log(`TransactionCache.getTransactionState(${txId}): found in local memory cache`)
      return tx
    }

    // Not in our in-memory cache
    // Try loading the transaction from our global (REDIS) cache
    if (VERBOSE) console.log(`TransactionCache.getTransactionState(${txId}): try to fetch from REDIS`)
    const tx1 = await schedulerForThisNode.getTransactionStateFromREDIS(txId)
    console.log(`tx1=`, tx1)

    // Compare to the transaction reconstructed from deltas
    // const tx2 = await TransactionPersistance.reconstructTransaction(txId)
    // const obj1 = tx1.asObject()
    // const obj2 = tx2.asObject()
    // const same = objectsAreTheSame(obj1, obj2)
    // console.log(`same=`, same)


    if (tx1) {
      // Found in the REDIS cache
      if (saveInLocalMemoryCache) {
        this.#cache.set(txId, tx1)
      }
      return tx1
    }

    // Not found in the global (REDIS) cache.
    // Try to select from the database.
    if (VERBOSE) console.log(`TransactionCache.getTransactionState(${txId}): try to fetch from database`)
    const sql = `SELECT json FROM atp_transaction_state WHERE transaction_id=?`
    const params = [ txId ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)
    if (rows.length > 0) {
      // Found in the Database
      if (VERBOSE) console.log(`Transaction state was found in the database`)
      const json = rows[0].json
      try {
        const tx3 = Transaction.transactionStateFromJSON(json)
        // const tx3 = JSON.parse(json)
        if (saveInLocalMemoryCache) {
          this.#cache.set(txId, tx3)
        }
        return tx3
      } catch (e) {
        // Serious error - notify the administrator
        //ZZZZZZ
        console.log(`Internal Error: Invalid JSON in atp_transaction_state [${txId}]`)
      }
    }

    // Not found anywhere
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
  async findTransactionByExternalId(owner, externalId, loadIfNecessary = true) {
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

    return this.getTransactionState(rows[0].transaction_id, loadIfNecessary)
  }

  /**
   * Remove the TransactionState from local memory.
   * 
   * @param {TransactionState} txId 
   */
  async removeFromCache(txId) {
    if (VERBOSE) console.log(`TransactionCache.removeFromCache(${txId})`)

    let tx = this.#cache.get(txId)
    if (tx) {
      this.#cache.delete(txId)
    } else {
      // Not in the cache
    }
  }

  /**
   * Move a transaction state from the local in-memory up to the "global"
   * cache in REDIS, which can be accessed by all nodes.
   * 
   * @param {string} txId Transaction ID
   * @param {boolean} shortTerm Move to database very soon
   */
  async moveToGlobalCache(txId, shortTerm=false) {
    if (VERBOSE) console.log(`TransactionCache.moveToGlobalCache(${txId}): persistence=${persistence}`)

    let tx = this.#cache.get(txId)
    if (tx) {

      // Save the transaction state in REDIS, and schedule it to be
      // saved to long term storage (and removal from REDIS).
      if (shortTerm) {
        await schedulerForThisNode.saveTransactionStateToREDIS(tx, PERSIST_FAST_DURATION)
      } else if (!!persistence) {
        await schedulerForThisNode.saveTransactionStateToREDIS(tx, PERSIST_REGULAR_DURATION)
      }

      // Delete from our memory cache here.
      await this.removeFromCache(txId)
    } else {
      // Not in the cache
    }
  }

  // /**
  //  *
  //  * @param {string} txId
  //  * @param {boolean} removeFromCache
  //  */
  // async persist(txId, removeFromCache = true) {
  //   // console.log(`TransactionCache.persist(${txId})`)

  //   const tx = this.#cache.get(txId)
  //   if (tx) {
  //     // console.log(`tx=`, tx)
  //     //ZZZZ Handle errors carefully here YARP2
  //     // console.log(`persist the transaction`)
  //     await TransactionPersistance.persistDeltas(tx)

  //     if (removeFromCache) {
  //       // console.log(`removing the transaction from the cache`)
  //       this.#cache.delete(txId)
  //     }
  //   }
  // }

  async size() {
    return this.#cache.size
  }

  /**
   *
   */
  async dump() {
    console.log(`Transaction cache:`)
    this.#cache.forEach((txId, tx) => {
      console.log(`  ${txId}, ${tx.toString()}`)
    })
    // for (let txId in this.#cache) {
    //   const tx = this.#cache[txId]
    //   console.log(`  ${txId}, ${tx.toString()}`)
    // }
  }
}

export default TransactionCache = new TransactionCache()