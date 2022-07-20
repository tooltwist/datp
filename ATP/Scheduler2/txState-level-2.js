/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

import dbupdate from "../../database/dbupdate"


/*
 *  Save the transaction state to long term storage.
 */
export async function saveTransactionState_level2(txId, json) {

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