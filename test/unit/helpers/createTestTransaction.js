/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import TransactionCacheAndArchive from "../../../ATP/Scheduler2/TransactionCacheAndArchive"
import XData from "../../../ATP/XData"

export default async function createTestTransaction () {
  const num = Math.round(Math.random() * 100000000000)
  // const num2 = Math.round(Math.random() * 100000000000)
  const owner = 'fred'
  const externalId = `e-${num}`
  const transactionType = 'example'
  // const nodeId = `n-${num2}`
  // const pipeline = 'demo/example'

  // const input = new XData({
  //   metadata: {
  //     externalId,
  //     transactionType,
  //     owner,
  //     nodeId,
  //     pipeline,
  //   },
  //   data: {
  //     value1: 'abc',
  //     value9: 'xyz'
  //   }
  // })

  // console.log(`input 111=`, input)
  const tx = await TransactionCacheAndArchive.newTransaction(owner, externalId, transactionType)
  const txId = tx.getTxId()
  return { txId, externalId, tx, owner }
}
