/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../.."
import { getNodeGroup } from "../../database/dbNodeGroup"
import { luaWakeupProcessing } from "./queuing/redis-retry"

const MIN_SCAN_INTERVAL = 5 * 1000 // ms

export class WakeupProcessor {
  // Config params
  #wakeupProcessing // Are we doing wakeup processing here?
  #wakeupPause

  constructor() {
    // console.log(`WakeupProcessor.contructor()`)
    this.#wakeupProcessing = 0 // We'll check the config every 30 seconds if disabled
    this.#wakeupPause = MIN_SCAN_INTERVAL
  }

  async checkConfig() {
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const group = await getNodeGroup(nodeGroup)
    // console.log(`group=`, group)

    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }

    const newWakeupProcessing = group.wakeupProcessing
    const newWakeupPause = Math.max(group.wakeupPause, MIN_SCAN_INTERVAL)

    // Display a nice message if something has changed
    if (this.#wakeupProcessing !== newWakeupProcessing || this.#wakeupPause !== newWakeupPause) {
      this.#wakeupProcessing = newWakeupProcessing
      this.#wakeupPause = newWakeupPause
      if (this.#wakeupProcessing) {
        console.log(` ✔ `.brightGreen + ` wakeup processing`)
        console.log(`      interval (ms):`, this.#wakeupPause)
      } else {
        console.log(` ✖ `.red + ` wakeup processing`)
        this.#wakeupPause = -1
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
      if (!this.#wakeupProcessing) {
        // We aren't doing wakeup processing from this node, but maybe that will change.
        setTimeout(scanLoop, 30 * 1000).unref()
        return
      }

      // Lua will look in the sleeping list for transactions who's wakeup time has
      // passed, and move them over to a processing queue (after setting status's, etc)
      await luaWakeupProcessing()

      // Prepare to run it again.
      // console.log(`wait a bit then try again`)
      setTimeout(scanLoop, this.#wakeupPause).unref()
    }//- scanLoop
    
    // Initial invocation
    // unref() allows the process to shut down if required, even though the timeout is still waiting.
    setTimeout(scanLoop, 0).unref()
  }
}
