import TransactionState from "../../DATP/ATP/Scheduler2/TransactionState"
import errors from 'restify-errors'
// import assert from 'assert'
import LongPoll from '../../DATP/ATP/Scheduler2/LongPoll'
import { STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING } from "../../DATP/ATP/Step"

const VERBOSE = 0

export async function transactionStatusByTxIdV1(req, res, next) {
  if (VERBOSE) {
    console.log(`\n---------------------------------`)
    console.log(`----- transactionStatusByTxIdV1()`)
  }
  // console.log(`req.tenant=`, req.tenant)

  if (typeof(req.tenant) === 'undefined') {
    res.send(new errors.InvalidArgumentError('Missing req.tenant (normally set by middleware)'))
    return next()
  }
  if (typeof(req.tenant.tenantId) === 'undefined') {
    res.send(new errors.InvalidArgumentError('Missing tenant.tenantId'))
    return next()
  }
  const tenantId = req.tenant.tenantId
  const txId = req.params.txId
  if (typeof(txId) !== 'string') {
    console.log(`txId=`, txId)
    res.send(new errors.InvalidArgumentError('Missing transactionId in URL'))
    return next()
  }

  // Get the current transaction summary
  const summary = await TransactionState.getSummary(tenantId, txId)

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
    await LongPoll.returnTxStatusAfterDelayWithPotentialEarlyReply(tenantId, txId, res, next)
  } else {

    // Not long polling - want the transaction details now, no matter what the status is.
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
