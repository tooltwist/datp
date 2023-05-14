/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../../.."
import { getNodeGroup } from "../../../database/dbNodeGroup"
import { RedisLua } from "../queuing/redis-lua"
import dbupdate from "../../../database/dbupdate"
import query from "../../../database/query"
import { luaTransactionsToArchive } from "../queuing/redis-transactions"


const IDLE_EMPTY_BATCHES = 10 // We switch to idle mode after this many empty batches


export class ArchiveProcessor {
  // Config params
  #archiveProcessing // Are we doing this in this node?
  #batchSize // Reload config if negative
  #archivePause
  #archivePauseIdle

  // We've go to idle mode after IDLE_EMPTY_BATCHES empty batches
  #emptyBatchCount

  constructor() {
    // console.log(`ArchiveProcessor.contructor()`)
    // Config
    this.#archiveProcessing = 0
    this.#batchSize = -1 // Load config
    this.#emptyBatchCount = 0
  }

  async checkConfig() {
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const group = await getNodeGroup(nodeGroup)
    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }
    // console.log(`group=`, group)
    const newArchiveProcessing = group.archiveProcessing
    const newBatchSize = Math.max(group.archiveBatchSize, 0)
    const newArchivePause = Math.max(group.archivePause, 0)
    const newArchivePauseIdle = Math.max(group.archivePauseIdle, 1000)

    // Display a nice message if something has changed
    if (
      this.#archiveProcessing !== newArchiveProcessing ||
      this.#batchSize !== newBatchSize ||
      this.#archivePause !== newArchivePause ||
      this.#archivePauseIdle !== newArchivePauseIdle
    ) {
      this.#archiveProcessing = newArchiveProcessing
      this.#batchSize = newBatchSize
      this.#archivePause = newArchivePause
      this.#archivePauseIdle = newArchivePauseIdle
      if (this.#archiveProcessing) {
        if (this.#batchSize > 0) {
          console.log(` ✔ `.brightGreen + `archive processing`)
          // console.log(`ArchiveProcessor:`)
          console.log(`          batchSize:`, this.#batchSize)
          console.log(`         pause (ms):`, this.#archivePause)
          console.log(`          idle (ms):`, this.#archivePauseIdle)
        } else {
          console.log(` ✖ `.red + `archive processing (because batch size is zero)`)
        }
      } else {
        console.log(` ✖ `.red + `archive processing`)
      }
    }
  }

  async start() {
    // console.log(`ArchiveProcessor.start()`)

    await RedisLua._checkLoaded()
    const nodeId = schedulerForThisNode.getNodeId()

    let persistedInPreviousIteration = [ ]
    const persistLoop = async () => {

      // If we are shutting down this node, do not continue archiving,
      // or we might end up with an incomplete save. But, we do need to
      // tell REDIS what we've already compeleted.
      if (schedulerForThisNode.shuttingDown()) {
        const batchSize = 0
        await luaTransactionsToArchive(persistedInPreviousIteration, nodeId, batchSize)
        return
      }

      // Are we archiving from this node?
      await this.checkConfig()
      if (!this.#archiveProcessing) {
        // We aren't archiving from this node now, but maybe that will change.
        setTimeout(persistLoop, 30 * 1000).unref()
        return
      }
      // console.log(`-----------------------`.red)
      // console.log(`ArchiveProcessor.loop()`.red)

      try {

        // Notify previously saved transaction states, and get a new batch to persist.
        // Note that the LUA script will designate just one node at a time as allowed
        // to do the archiving. During that period of time all other nodes will get
        // an empty list of transaction states if they ask.
        const transactions = await luaTransactionsToArchive(persistedInPreviousIteration, nodeId, this.#batchSize)
        persistedInPreviousIteration = [ ]

        // If there are no transactions to be persisted, we might want to go into idle mode.
        if (transactions.length === 0) {
          // console.log(`empty batch`)

          // Empty batch
          this.#emptyBatchCount++
          if (this.#emptyBatchCount >= IDLE_EMPTY_BATCHES) {
            // if (this.#emptyBatchCount === IDLE_EMPTY_BATCHES) console.log(`Archiving entering IDLE MODE`)
            // console.log(`sleep a lot`)
            setTimeout(persistLoop, this.#archivePauseIdle).unref()
          } else {
            // console.log(`sleep a bit`)
            setTimeout(persistLoop, this.#archivePause).unref()
          }
          return
        }
        this.#emptyBatchCount = 0
        // console.log(`transactions=`, transactions)
        // console.log(`transactions.length=`, transactions.length)

        // Archive each transaction's state
        let cntSaved = 0
        for (const item of transactions) {

          if (item[0] === 'transaction') {

            // We have the transaction state to save
            const txId = item[1]
            const json = item[2]
            // const state = JSON.parse(json)
            // console.log(`PERSIST ${txId}:`, JSON.stringify(state, '', 2))

            try {
              await archiveTransactionState(txId, json)
              persistedInPreviousIteration.push(txId)
              cntSaved++
            } catch (e) {
              console.log(`Could not save state of transaction ${txId}`, e)
            }
          } else {

            // This item is not a transaction state
            console.log(`Error while getting transactions to archive.`)
            console.log(`item=`, item)
          }
        }//- for
  
  
        if (cntSaved > 0) {
         console.log(`Archived ${cntSaved} transaction states`)
        }
  
      } catch (e) {
        console.log(`Error while archiving transaction states`, e)
      }
      // Prepare to run it again.
      // console.log(`wait a bit then try again`)
      setTimeout(persistLoop, this.#archivePause).unref()
    }//- persistLoop
    
    // Initial invocation
    // unref() allows the process to shut down if required, even though the timeout is still waiting.
    setTimeout(persistLoop, this.#archivePause).unref()
  }
}


/*
 *  Save the transaction state to long term storage.
 */
export async function archiveTransactionState(txId, json) {

  try {
    /*
     *  Insert transaction state into the database.
     */
    let sql = `INSERT INTO atp_transaction_state (transaction_id, json) VALUES (?, ?)`
    let params = [ txId, json ]
    let result2 = await dbupdate(sql, params)
    // console.log(`result2=`, result2)
    if (result2.affectedRows !== 1) {
      //ZZZZZZ Notify the admin
      console.log(`SERIOUS ERROR: archiveTransactionState: could not insert into DB [${txId}]`, e)
      return
    }

  } catch (e) {
    if (e.code !== 'ER_DUP_ENTRY') {
      //ZZZZZZ Notify the admin
      console.log(`SERIOUS ERROR: archiveTransactionState: could not insert into DB [${txId}]`, e)
      return
    }

    /*
     *  Already in DB - need to update
     */
    // console.log(`Need to update`)
    const sql = `UPDATE atp_transaction_state SET json=? WHERE transaction_id=?`
    const params = [ json, txId ]
    const result2 = await dbupdate(sql, params)
    // console.log(`result2=`, result2)
    if (result2.affectedRows !== 1) {
      //ZZZZZZ Notify the admin
      console.log(`SERIOUS ERROR: archiveTransactionState: could not update DB [${txId}]`, e)
      return
    }
  }
}//- archiveTransactionState

export async function findArchivedTransactions(page, pagesize, status, filter) {
  const sql = `SELECT
    transaction_id AS txId, ` +
    // json,
    `creation_time AS created,
    update_time AS updated
  FROM atp_transaction_state
  ORDER BY created DESC LIMIT ?, ?`

  const params = [ page, pagesize ]
  const result = await query(sql, params)
  // console.log(`result=`, result)
  return result
}

// export async function getArchivedTransactionState(txId) {
//   console.log(`getArchivedTransactionState(${txId})`)

//   const sql = `SELECT
//     transaction_id AS txId,
//     json,
//     creation_time AS created,
//     update_time AS updated
//   FROM atp_transaction_state
//   WHERE transaction_id = ?`

//   const params = [ txId ]
//   const result = await query(sql, params)
//   console.log(`result=`, result)
//   return result
// }
