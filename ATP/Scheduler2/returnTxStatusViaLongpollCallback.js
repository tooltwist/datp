import assert from 'assert'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import LongPoll from './LongPoll'

require('colors')

export const RETURN_TX_STATUS_WITH_LONGPOLL_CALLBACK = `returnTxStatusWithLongPoll`

export async function returnTxStatusWithLongPollCallback (callbackContext, data) {
  if (PIPELINES_VERBOSE) console.log(`==> returnTxStatusWithLongPollCallback()`.magenta, callbackContext, data)

  assert(data.owner)
  assert(data.txId)

  const sent = await LongPoll.tryToReply(data.txId)
  // console.log(`sent=`, sent)
}//- returnTxStatusWithLongPollCallback
