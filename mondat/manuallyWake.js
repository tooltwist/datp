/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

import { luaGetSwitch, luaManuallyWake, luaSetSwitch } from "../ATP/Scheduler2/queuing/redis-retry"

const VERBOSE = 0

export async function mondatRoute_manuallyWakeV1(req, res, next) {
  if (VERBOSE) console.log(`mondatRoute_manuallyWakeV1()`.yellow)

  const txId = req.params.txId
  console.log(`txId=`, txId)
  const result = await luaManuallyWake(txId)
  res.send(result)
  return next()
}//- mondatRoute_manuallyWakeV1


export async function mondatRoute_manuallySetSwitchV1(req, res, next) {
  if (VERBOSE) console.log(`mondatRoute_manuallySetSwitchV1()`.yellow)

  const txId = req.params.txId
  // console.log(`txId=`, txId)
  const switchName = req.params.switchName
  // console.log(`switchName=`, switchName)
  const value = req.params.value
  // console.log(`value=`, value)

  const result = await luaSetSwitch(txId, switchName, value)
  // console.log(`luaSetSwitch() result=`, result)

  res.send(result)
  return next()
}//- mondatRoute_manuallySetSwitchV1

// Remove this once switch testing is complete
export async function mondatRoute_zzzGetSwitchV1(req, res, next) {
  if (VERBOSE) console.log(`mondatRoute_zzzGetSwitchV1()`.yellow)

  const txId = req.params.txId
  // console.log(`txId=`, txId)
  const switchName = req.params.switchName
  // console.log(`switchName=`, switchName)

  const acknowledgeValue = true
  const result = await luaGetSwitch(txId, switchName, acknowledgeValue)
  // console.log(`luaGetSwitch() result=`, result)

  res.send(result)
  return next()
}//- mondatRoute_zzzGetSwitchV1
