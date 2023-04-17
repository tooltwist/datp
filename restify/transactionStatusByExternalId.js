import Transaction from "../../DATP/ATP/Scheduler2/TransactionState"
import LongPoll from "../../DATP/ATP/Scheduler2/LongPoll"
import errors from 'restify-errors'
import assert from 'assert'
import { STEP_RUNNING, STEP_SLEEPING, STEP_QUEUED } from "../../DATP/ATP/Step"

const VERBOSE = 0

export async function transactionStatusByExternalIdV1(req, res, next) {
  if (VERBOSE) {
    console.log(`\n---------------------------------`)
    console.log(`----- transactionStatusByExternalIdV1()`)
  }

  const { tenantId } = req.tenant
  const externalId = req.params.externalId
  assert(externalId)

  // Get the current transaction summary (including the transactionID)
  const summary = await Transaction.getSummaryByExternalId(tenantId, externalId)

  // Is long polling requested?
  const longPolling = (req.body && req.body.metadata && req.body.metadata.reply === 'longpoll') || (req.query && req.query.reply === 'longpoll')
  if (longPolling) {

    // If the transaction is not complete, delay responding - we may have a reply soon
    if (VERBOSE) console.log(`----- via longpoll`)
    // Maybe we already have a reply?
    if (summary) {
      const status = summary.metadata.status
      if (VERBOSE) console.log(`----- current status=${status}`)
      if (status !== STEP_QUEUED && status !== STEP_RUNNING && status !== STEP_SLEEPING) {
        if (VERBOSE) console.log(`----- already complete, no need to wait`)
        res.send(summary)
        return next()
      }
    }
    // Wait, hopefully for the transaction to complete, or else for a timeout
    await LongPoll.returnTxStatusAfterDelayWithPotentialEarlyReply(tenantId, summary.metadata.txId, res, next)
  } else {

    // Return current status
    if (VERBOSE) console.log(`----- immediate reply`)
    if (summary) {
      if (VERBOSE) console.log(`----- status=${summary.metadata.status}`)
      res.send(summary)
      return next()
    } else {
      res.send(new errors.NotFoundError('Unknown transaction'))
      return next()
    }
  }
}