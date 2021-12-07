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
