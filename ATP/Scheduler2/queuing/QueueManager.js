export class QueueManager {
  constructor() {
  }

  async enqueue(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.enqueue()`)
  }

  /**
   * @returns The item at the head of the queue, removed from the queue
   */
  async dequeue(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.dequeue()`)
  }

  async queueLength(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.length()`)
  }

  async close(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.close()`)
  }
}
