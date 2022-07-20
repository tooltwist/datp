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
import { isDevelopmentMode } from '../../datp-constants'
import { saveTransactionState_level2 } from './txState-level-2'
// import { objectsAreTheSame } from '../../lib/objectDiff'

const PERSIST_FAST_DURATION = 10 // Almost immediately
const PERSIST_REGULAR_DURATION = 120 // Two minutes

const VERBOSE = 0

class TransactionCache {
  #cacheId // To check we only have one cache!

  constructor() {
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
    // this.#cache.set(txId, tx)

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

    // let tx = this.#cache.get(txId)
    // if (tx) {

    //   // Already in our in-memory cache
    //   if (VERBOSE) console.log(`TransactionCache.getTransactionState(${txId}): found in local memory cache`)
    //   return tx
    // }

    // if (!loadIfNecessary) {
    //   return null
    // }

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
      // if (saveInLocalMemoryCache) {
      //   this.#cache.set(txId, tx1)
      // }
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
        // if (saveInLocalMemoryCache) {
        //   this.#cache.set(txId, tx2)
        // }
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
      console.log(`WARNING: Transaction ${txId} had to be resurrected from deltas. Did the server die?`)
      // if (saveInLocalMemoryCache) {
      //   this.#cache.set(txId, tx3)
      // }

      // Save the transaction state in REDIS, and schedule it to be
      // saved to long term storage (and removal from REDIS).
      await schedulerForThisNode.saveTransactionState_level1(tx3)
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
}

export default TransactionCache = new TransactionCache()



export async function PERSIST_TRANSACTION_STATE(tx) {
  if (VERBOSE) console.log(`TransactionCache.PERSIST_TRANSACTION_STATE(${tx.txId})`)

  if (await isDevelopmentMode()) {

    // In development mode, we'll write to the database immediately.
    const txId = tx.getTxId()
    const json = tx.stringify()
    saveTransactionState_level2(txId, json)
  } else {

    // Save the transaction state in REDIS, and schedule it to be
    // saved to long term storage (and removal from REDIS).
    await schedulerForThisNode.saveTransactionState_level1(tx)
  }
}
