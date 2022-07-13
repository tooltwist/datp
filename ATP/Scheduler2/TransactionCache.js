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
import pause from '../../lib/pause'
// import { objectsAreTheSame } from '../../lib/objectDiff'

const PERSIST_FAST_DURATION = 10 // Almost immediately
const PERSIST_REGULAR_DURATION = 120 // Two minutes

const VERBOSE = 0

class TransactionCache {
  #cache
  #cacheId // To check we only have one cache!

  constructor() {
    this.#cache = new Map() // txId => Transaction2
    this.#cacheId = GenerateHash('cache')
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

  assertNotInCache(txId) {
    if (this.#cache.has(txId)) {
      console.trace(`Transaction state for ${txId} should not be in the local transaction cache [${this.#cacheId}]`)
      system.exit(1)
    }
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

    if (!loadIfNecessary) {
      return null
    }

    // Not in our in-memory cache
    // Try loading the transaction from our global (REDIS) cache
    if (VERBOSE) console.log(`TransactionCache.getTransactionState(${txId}): try to fetch from REDIS`)
    // await pause(20)//ZZZZZZ Hack to give REDIS time to sync or flush or whatever...
    const tx1 = await schedulerForThisNode.getTransactionStateFromREDIS(txId)

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
      // YARP248
      // console.log(`${schedulerForThisNode.getNodeId()}: yarp loaded ${txId} from REDIS (${tx1.getDeltaCounter()})`)
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
        const tx2 = Transaction.transactionStateFromJSON(json)
        if (saveInLocalMemoryCache) {
          this.#cache.set(txId, tx2)
        }
        return tx2
      } catch (e) {
        // Serious error - notify the administrator
        //ZZZZZZ
        console.log(`Internal Error: Invalid JSON in atp_transaction_state [${txId}]`)
      }
    }

    // Not in the database. Can we reconstruct it from the deltas?
    const tx3 = await TransactionPersistance.reconstructTransaction(txId)
    // console.log(`tx3=`, tx3)
    if (tx3) {
      //ZZZZZ This should be raised as an administrator's notification.
      console.log(`WARNING: Transaction ${txId} had to be resurrected from deltas. Why was this required?`)
      if (saveInLocalMemoryCache) {
        this.#cache.set(txId, tx3)
      }
      return tx3
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

    // let tx = this.#cache.get(txId)
    // if (tx) {
      const have1 = this.#cache.has(txId)
      const rv = this.#cache.delete(txId)
      const have2 = this.#cache.has(txId)
      if (have1 && have2) {
        console.log(`YARP TX CACHE DELETE IS NOT WORKING`)
      }
      return rv
    // } else {
    //   // Not in the cache
    // }
  }

  /**
   * Move a transaction state from the local in-memory up to the "global"
   * cache in REDIS, which can be accessed by all nodes.
   * 
   * @param {string} txId Transaction ID
   * @param {boolean} shortTerm Move to database very soon
   */
  async moveToGlobalCache(txId, shortTerm=false) {
    if (VERBOSE) console.log(`TransactionCache.moveToGlobalCache(${txId}): shortTerm=${shortTerm}`)

    let tx = this.#cache.get(txId)
    if (tx) {

      // Save the transaction state in REDIS, and schedule it to be
      // saved to long term storage (and removal from REDIS).
      const timeTillPersist = shortTerm ? PERSIST_FAST_DURATION : PERSIST_REGULAR_DURATION
      await schedulerForThisNode.saveTransactionStateToREDIS(tx, timeTillPersist)

      // Delete from our memory cache here.
      // await this.removeFromCache(txId)
      this.#cache.delete(txId)

    } else {
      // Not found in the cache
    }
  }

  /**
   * Add a transaction state to the cache.
   * This is used when the transaction state is passed between nodes within an event.
   * @param {Transaction} tx 
   */
  async addToCache(tx) {
    if (VERBOSE) console.log(`TransactionCache.addToCache():`, tx)

    const txId = tx.getTxId()
    assert(txId)
    this.#cache.set(txId, tx)
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
    return this.#cache.size // Yep, not a function.
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