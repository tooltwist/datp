/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
export class QueueManager {
  constructor() {
  }

  async enqueue(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.enqueue()`)
  }

  /**
   * @returns The item at the head of the queue, removed from the queue
   */
  async dequeue(queues, numEvents, block=false) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.dequeue()`)
  }

  async queueLength(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.length()`)
  }

  async close(queueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.close()`)
  }

  async queueLengths() {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.queueLengths()`)
  }

  async repeatEventDetection(key, interval) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.repeatEventDetection()`)
  }

  async registerNode(nodeId, status) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.registerNode()`)
  }

  async getNodeIds() {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.getNodeIds()`)
  }

  async getNodeDetails() {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.getNodeDetails()`)
  }

  async moveElementsToAnotherQueue(fromQueueName, toQueueName) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.moveElementsToAnotherQueue()`)
  }

  async setTemporaryValue(key, value, duration) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.setTemporaryValue()`)
  }

  async getTemporaryValue(key) {
    throw new Error(`Fatal error: class ${this.constructor.name} does not extend QueueManager.getTemporaryValue()`)
  }

}
