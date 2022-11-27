/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
export function flowMsg(txState, msg, flowIndex=-1) {
  // console.log(`flowMsg(${msg})`)
  const flowLength = txState.vog_getFlowLength()
  let level = -1 // transaction level
  if (flowLength === 0) {
    level = -1
  } else if (flowIndex < 0) {
    // Use the latest
    // console.log(`flowIndex=`, flowIndex)
    //const flowRecord = txState.vog_getFlowRecord(flowIndex)
    const flowRecord = txState.vog_getFlowRecord(flowLength - 1)
    // console.log(`flowRecord=`, flowRecord)
    const stepRecord = txState.vog_getStepRecord(flowRecord.stepId)
    // console.log(`stepRecord=`, stepRecord)
    level = stepRecord.level
  } else if (flowIndex < flowLength) {
    const flowRecord = txState.vog_getFlowRecord(flowIndex)
    const stepRecord = txState.vog_getStepRecord(flowRecord.stepId)
    level = stepRecord.level
  } else {
    const err = `Invalid flow index [${flowIndex}]`
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

export function flow2Msg(txState, msg, f2i=-1) {
  console.log(`flow2Msg(${msg}, f2i=${f2i})`)
  const flowLength = txState.vf2_getF2Length()
  let level = -1 // transaction level
  if (flowLength === 0) {
    level = -1
  } else if (f2i < 0) {
    // Use the latest
    // console.log(`f2i=`, f2i)
    const f2 = txState.vf2_getF2(flowLength - 1)
    // console.log(`f2=`, f2)
    // const stepRecord = txState.vog_getStepRecord(f2.stepId)
    // console.log(`stepRecord=`, stepRecord)
    level = f2.l
  } else if (f2i < flowLength) {
    console.log(`f2i=`, f2i)
    const f2 = txState.vf2_getF2(f2i)
    // console.log(`f2=`, f2)
    // const stepRecord = txState.vog_getStepRecord(f2.stepId)
    // console.log(`stepRecord=`, stepRecord)
    level = f2.l
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

