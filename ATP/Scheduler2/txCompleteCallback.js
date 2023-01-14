/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { GO_BACK_AND_RELEASE_WORKER } from "./Worker2"
import { RedisQueue } from "./queuing/RedisQueue-ioredis"
import { FLOW_VERBOSE } from "./queuing/redis-lua"


export const TX_COMPLETE_CALLBACK = `txComplete`

export async function txCompleteCallback (tx, f2i , nodeInfo, worker) {
  if (FLOW_VERBOSE) flow2Msg(tx, `Callback txCompleteCallback(f2i=${f2i})`, f2i)
  
  // Save the final step's output, status and note as the transaction's result
  const f2Status = tx.vf2_getStatus(f2i)
  const f2Note = tx.vf2_getNote(f2i)
  const f2Output = tx.vf2_getOutput(f2i)
  tx.vog_setToComplete(f2Status, f2Note, f2Output)
  
  // Update the root f2
  const rootF2 = tx.vf2_getF2(0)
  rootF2.ts3 = Date.now()

  // console.log(`

  

  // FROM HERE WE NEED TO EITHER SCHEDULE REPLY VIA...
  
  // - WEBHOOK IF REQUESTED, and
  
  // - POSSIBLY LONG POLL, VIA PUB/SUB
  
  // Once these are requested, we've finished processing the transaction.
  
  // Yay!
  
  

  // (finished ${Date.now() % 10000})
  
  // `.magenta)



  //VOGTX
  // console.log(`txCompleteCallback tx=`, JSON.stringify(tx.asObject(), '', 2))
  const redisLua = await RedisQueue.getRedisLua()
  await redisLua.luaTransactionCompleted(tx)




  return GO_BACK_AND_RELEASE_WORKER
}
