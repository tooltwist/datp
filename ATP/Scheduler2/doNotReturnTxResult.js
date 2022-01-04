import { PIPELINES_VERBOSE } from "../hardcoded-steps/PipelineStep"

export const DO_NOT_RETURN_TX_RESULT_CALLBACK = `doNotReturnTxResultCallback`

/**
 * This callback is called when the tranaction is finished, when
 * short polling is required. It does nothing, because the client
 * will be independantly polling to get the transaction status.
 */
export async function doNotReturnTxResultCallback (callbackContext, data) {
  if (PIPELINES_VERBOSE) console.log(`==> doNotReturnTxResultCallback()`.magenta, callbackContext, data)
  // if (PIPELINES_VERBOSE > 1) {
  //   console.log(`callbackContext=`, callbackContext)
  //   console.log(`data=`, data)
  // }
}
