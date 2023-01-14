/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { F2ATTR_LEVEL } from "./TransactionState"

export function flow2Msg(txState, msg, f2i=-1) {
  // console.log(`flow2Msg(${msg}, f2i=${f2i})`)
  const flowLength = txState.vf2_getF2Length()
  let level = -1 // transaction level
  if (flowLength === 0) {
    level = -1
  } else if (f2i < 0) {
    // Use the latest
    // console.log(`f2i=`, f2i)
    const f2 = txState.vf2_getF2(flowLength - 1)
    // console.log(`f2=`, f2)
    // const stepRecord = txState.vog_getStepRecord(f2[F2ATTR_STEPID])
    // console.log(`stepRecord=`, stepRecord)
    level = f2[F2ATTR_LEVEL]
  } else if (f2i < flowLength) {
    // console.log(`f2i=`, f2i)
    const f2 = txState.vf2_getF2(f2i)
    // console.log(`f2=`, f2)
    // const stepRecord = txState.vog_getStepRecord(f2[F2ATTR_STEPID])
    // console.log(`stepRecord=`, stepRecord)
    level = f2[F2ATTR_LEVEL]
  } else {
    const err = `Invalid flow index [${f2i}]`
    console.trace(err)
    throw new Error(err)
  }
  // console.log(`level=`, level)

  switch (level) {
    case -1:
      console.log(`${msg}`.bgRed.brightWhite)
      break

    case 0:
      console.log(`${msg}`.brightYellow)
      break

    case 1:
      console.log(`${msg}`.brightBlue)
      break

    case 2:
      console.log(`${msg}`.brightGreen)
      break

    default:
      console.log(`${msg}`)
      break
  }
}
