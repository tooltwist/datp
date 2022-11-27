/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../../.."
import { getNodeGroup } from "../../../database/dbNodeGroup"
import { RedisLua } from "../queuing/redis-lua"
import dbupdate from "../../../database/dbupdate"


const IDLE_EMPTY_BATCHES = 10 // We switch to idle mode after this many empty batches


export class ArchiveProcessor {
  // Config params
  #batchSize // Reload config if negative
  #archivePause
  #archivePauseIdle

  // We've go to idle mode after IDLE_EMPTY_BATCHES empty batches
  #emptyBatchCount

  constructor() {
    // console.log(`ArchiveProcessor.contructor()`)
    // Config
    this.#batchSize = -1 // Load config
    this.#emptyBatchCount = 0
  }

  async checkConfig() {
    if (this.#batchSize >= 0) {
      return
    }
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const group = await getNodeGroup(nodeGroup)
    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }
    // console.log(`group=`, group)
    this.#batchSize = Math.max(group.archiveBatchSize, 0)
    this.#archivePause = Math.max(group.archivePause, 0)
    this.#archivePauseIdle = Math.max(group.archivePauseIdle, 1000)

    if (this.#batchSize < 1) {
      console.log(` ✖ `.red + `archiving transaction states`)
    } else {
      console.log(` ✔ `.brightGreen + `archiving transaction states`)
      // console.log(`ArchiveProcessor:`)
      console.log(`      batchSize:`, this.#batchSize)
      console.log(`          pause:`, this.#archivePause)
      console.log(`           idle:`, this.#archivePauseIdle)
    }
  }

  async start() {
    // console.log(`ArchiveProcessor.start()`)

    const lua = new RedisLua()
    await RedisLua._checkLoaded()
    const nodeId = schedulerForThisNode.getNodeId()

    let persistedInPreviousIteration = [ ]
    const persistLoop = async () => {

      // If we are shutting down this node, do not continue archiving,
      // or we might end up with an incomplete save. But, we do need to
      // tell REDIS what we've already compeleted.
      if (schedulerForThisNode.shuttingDown()) {
        const batchSize = 0
        await lua.transactionsToArchive(persistedInPreviousIteration, nodeId, batchSize)
        return
      }

      // Are we archiving from this node?
      await this.checkConfig()
      if (this.#batchSize < 1) {
        // We aren't archiving from this node, but maybe that will change.
        setTimeout(persistLoop, 10 * 1000).unref()
        return
      }
      // console.log(`-----------------------`.red)
      // console.log(`ArchiveProcessor.loop()`.red)

      try {

        // Notify previously saved transaction states, and get a new batch to persist.
        // Note that the LUA script will designate just one node at a time as allowed
        // to do the archiving. During that period of time all other nodes will get
        // an empty list of transaction states if they ask.
        const transactions = await lua.transactionsToArchive(persistedInPreviousIteration, nodeId, this.#batchSize)
        persistedInPreviousIteration = [ ]

        // If there are no transactions to be persisted, we might want to go into idle mode.
        if (transactions.length === 0) {
          // console.log(`empty batch`)

          // Empty batch
          this.#emptyBatchCount++
          if (this.#emptyBatchCount >= IDLE_EMPTY_BATCHES) {
            if (this.#emptyBatchCount === IDLE_EMPTY_BATCHES) console.log(`Archiving entering IDLE MODE`)
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
      console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not insert into DB [${txId}]`, e)
      return
    }

  } catch (e) {
    if (e.code !== 'ER_DUP_ENTRY') {
      //ZZZZZZ Notify the admin
      console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not insert into DB [${txId}]`, e)
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
      console.log(`SERIOUS ERROR: persistTransactionStatesToLongTermStorage: could not update DB [${txId}]`, e)
      return
    }
  }

}