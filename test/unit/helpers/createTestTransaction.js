import TransactionCache from "../../../ATP/Scheduler2/TransactionCache"
import XData from "../../../ATP/XData"

export default async function createTestTransaction () {
  const num = Math.round(Math.random() * 100000000000)
  const num2 = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const transactionType = 'example'
  const owner = 'fred'
  const nodeId = `n-${num2}`
  const pipeline = 'demo/example'

  const input = new XData({
    metadata: {
      externalId,
      transactionType,
      owner,
      nodeId,
      pipeline,
    },
    data: {
      value1: 'abc',
      value9: 'xyz'
    }
  })

  // console.log(`input 111=`, input)
  const tx = await TransactionCache.newTransaction(owner, externalId)
  const txId = tx.getTxId()
  return { txId, externalId, tx, owner }
}
