/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../.."
import { getNodeGroup } from "../../database/dbNodeGroup"
import { luaWakeupProcessing } from "./queuing/redis-retry"

const DEFAULT_SCAN_INTERVAL = 7 * 1000 // ms

export class WakeupProcessor {
  // Config params
  #scanInterval // Reload config if negative

  constructor() {
    // console.log(`WakeupProcessor.contructor()`)
    this.#scanInterval = -2
  }

  async checkConfig() {
    if (this.#scanInterval >= 0) {
      // Already doing s
      return
    }
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const group = await getNodeGroup(nodeGroup)
    // console.log(`group=`, group)

    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }

    // If this node is archiving, we'll also use it fro wake Processing
    const archiveBatchSize = Math.max(group.archiveBatchSize, 0)
    const newScanInterval = archiveBatchSize > 0 ? DEFAULT_SCAN_INTERVAL : -1

    if (newScanInterval != this.#scanInterval) {
      this.#scanInterval = archiveBatchSize > 0 ? DEFAULT_SCAN_INTERVAL : -1
      // console.log(`this.#scanInterval=`, this.#scanInterval)
      if (this.#scanInterval < 1) {
        console.log(` ✖ `.red + ` wakeup daemon`)
      } else {
        console.log(` ✔ `.brightGreen + ` wakeup daemon`)
        console.log(`      interval (ms):`, this.#scanInterval)
      }
    }
  }

  async start() {
    // console.log(`WakeupProcessor.start()`)

    const scanLoop = async () => {
      // console.log(`WakeupProcessor.scanLoop()`)

      // If we are shutting down this node, do not continue waking transactions.
      if (schedulerForThisNode.shuttingDown()) {
        return
      }

      // Are we archiving from this node?
      await this.checkConfig()
      if (this.#scanInterval < 1) {
        // We aren't archiving from this node, but maybe that will change.
        setTimeout(scanLoop, 30 * 1000).unref()
        return
      }

      // Lua will look in the sleeping list for transactions who's wakeup time has
      // passed, and move them over to a processing queue (after setting status's, etc)
      await luaWakeupProcessing()

      // Prepare to run it again.
      // console.log(`wait a bit then try again`)
      setTimeout(scanLoop, this.#scanInterval).unref()
    }//- scanLoop
    
    // Initial invocation
    // unref() allows the process to shut down if required, even though the timeout is still waiting.
    setTimeout(scanLoop, this.#scanInterval).unref()
  }
}
