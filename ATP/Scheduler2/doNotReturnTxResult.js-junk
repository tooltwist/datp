/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
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
