
export class QueueItem {
  #value
  #next
  #previous
  #isInQueue

  constructor(value) {
    this.#value = value
  }

  __getNext() {
    return this.#next
  }

  __getPrevious() {
    return this.#previous
  }

  __setNext(next) {
    this.#next = next
  }

  __setPrevious(previous) {
    this.#previous = previous
  }

  __setIsInQueue(inQueue) {
    if (this.#isInQueue) {
      // Already in a queue
      if (inQueue) {
        throw new Error(`item is already in a queue`)
      }
    } else {
      // Not in a queue
      if (!inQueue) {
        throw new Error(`item is not in a queue`)
      }
    }
    this.#isInQueue = inQueue
  }

  // toString() {
  get [Symbol.toStringTag]() {
    return this.#value
  }
}
