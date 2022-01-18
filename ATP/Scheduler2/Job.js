/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { QueueItem } from "./QueueItem";

export class Job extends QueueItem {
  #input

  constructor(input) {
    super()
    this.#input = input
  }

  getInput() {
    return this.#input
  }

  get [Symbol.toStringTag]() {
    const txId = this.#input.metadata.txId
    const stepId = this.#input.metadata.stepId
    return `${txId} : ${stepId}`
  }

}
