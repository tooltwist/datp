/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { GO_BACK_AND_RELEASE_WORKER } from "./Worker2"
import { RedisQueue } from "./queuing/RedisQueue-ioredis"
import { FLOW_VERBOSE } from "./queuing/redis-lua"
import { flow2Msg } from "./flowMsg"
import { luaTransactionCompleted } from "./queuing/redis-txCompleted"


export const TX_COMPLETE_CALLBACK = `txComplete`

export async function txCompleteCallback (tx, f2i, worker) {
  if (FLOW_VERBOSE) flow2Msg(tx, `Callback txCompleteCallback(f2i=${f2i})`, f2i)
  
  // Save the final step's output, status and note as the transaction's result
  const f2Status = tx.vf2_getStatus(f2i)
  const f2Note = tx.vf2_getNote(f2i)
  const f2Output = tx.vf2_getOutput(f2i)
  tx.vog_setToComplete(f2Status, f2Note, f2Output)
  
  // Update the root f2
  const rootF2 = tx.vf2_getF2(0)
  rootF2.ts3 = Date.now()

  await luaTransactionCompleted(tx)
  return GO_BACK_AND_RELEASE_WORKER
}
