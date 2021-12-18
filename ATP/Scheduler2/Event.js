const { QueueItem } = require("./QueueItem")

export class Event extends QueueItem {
  #eventType
  #eventData

  /**
   *
   * @param {string} eventType
   * @param {XData} data
   */
  constructor(eventType, data) {
    super()
    // super({ eventType, data })
    this.#eventType = eventType
    this.#eventData = data
  }

  getType() {
    return this.#eventType
  }

  getData() {
    return this.#eventData
  }


  // toString() {
  get [Symbol.toStringTag]() {
    return `${this.#eventType}, ${JSON.stringify(this.#eventData, '', 2)}`
  }
}
