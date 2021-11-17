import Step from '../Step'
import Scheduler from '../Scheduler'
import TxData from '../TxData'
import ResultReceiver from '../ResultReceiver'
import ResultReceiverRegister from '../ResultReceiverRegister'

import assert from 'assert'

const CHILD_PIPELINE_COMPLETION_HANDLER_NAME = 'child-pipeline-completion-handler'


/**
 * This ResultReceiver is called when the transaction is complete. It persists
 * our transaction details (i.e. updates the database) before calling the
 * ResultReceiver for the code that called ATP.initiateTransaction() to
 * start the transaction.
 */
class ChildPipelineCompletionHandler extends ResultReceiver {
  constructor() {
    super()
  }
  async haveResult(contextForCompletionHandler, status, note, response) {
    assert(response instanceof TxData)
    console.log(`<<<<    ChildPipelineCompletionHandler.haveResult(${status}, ${note})  `.white.bgBlue.bold)

    try {
      // Update the transaction status
      const txId = contextForCompletionHandler.txId
      console.log(`FINISHED ${txId}`)

      const parentStepId = contextForCompletionHandler.parentStepId
      const parentIndexEntry = await Scheduler.getStepEntry(parentStepId)
      if (!parentIndexEntry) {
        throw new Error(`Internal error 827772: could not find transfer step in Scheduler (${parentStepId})`)
      }
      const parentInstance = await parentIndexEntry.getStepInstance()

      // Complete the parent step, based on what the child pipline returned.
      assert(status !== Step.FAIL) // Should not happen. Pipelines either rollback or abort.
      switch (status) {
        case Step.COMPLETED:
          console.log(`Child pipeline is COMPLETED`)
          return await parentInstance.succeeded(note, response)

        case Step.ABORT:
          console.log(`Child pipeline is ABORT`)
          return await parentInstance.failed(note, response)

        default:
          console.log(`------- Step completed with an unrecognised status ${status}`)
          return await parentInstance.exceptionInStep(`Child transfer pipeline returned unknown status [${status}]`, { })
      }

    } catch (e) {
      console.trace(`Error in ChildPipelineCompletionHandler`, e)
      throw e
    }
  }//- haveResult
}

async function register() {
  await ResultReceiverRegister.register(CHILD_PIPELINE_COMPLETION_HANDLER_NAME, new ChildPipelineCompletionHandler())
}//- register

const myDef = {
  register,
  CHILD_PIPELINE_COMPLETION_HANDLER_NAME
}
export default myDef
