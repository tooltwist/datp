/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import TransactionCache from "../../../ATP/Scheduler2/txState-level-1"
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
  const tx = await TransactionCache.newTransaction(owner, externalId, transactionType)
  const txId = tx.getTxId()
  return { txId, externalId, tx, owner }
}
