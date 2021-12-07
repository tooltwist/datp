
export default class Worker {
  #id
  #type
  #inUse

  constructor(id, type) {
    this.#id = id
    this.#type = type
    this.#inUse = false
  }

  getId() {
    return this.#id
  }

  getType() {
    return this.#type
  }

  getInUse() {
    return this.#inUse
  }

  setInUse(inUse) {
    if (this.#inUse) {
      // Is in use
      if (inUse) {
        throw new Error(`Worker is already in use [${this.#id}, ${this.#type}]`)
      }

    } else {
      // Not in use
      if (!inUse) {
        throw new Error(`Worker is already not in use [${this.#id}, ${this.#type}]`)
      }
    }
    this.#inUse = inUse
  }

  inUse() {
    return this.#inUse
  }

}