import GenerateHash from "./GenerateHash"

export default class TxData {
  #data
  #json

  constructor(initialData) {
    // console.log(`TxData.constructor()`, initialData)
    // this.#id = GenerateHash('tx')
    if (initialData === null) {
      initialData = { }
    }
    if (initialData instanceof TxData) {
      // console.log(`YARP cloning TxData`)
      initialData = initialData.getJson() // Will clone via parsing
      // console.log(`initialData becomes`, initialData)
    }
    switch (typeof(initialData)) {
      case 'string':
        // We've been passed JSON
        this.#json = initialData
        this.#data = null
        break
      case 'object':
        // We've been passed an object
        this.#json = null
        this.#data = initialData
        break
      default:
        throw new Error(`TxData.constructor() must be passed JSON or an Object.`)
    }
    // console.log(`this=`, this)
  }

  getData() {
    if (this.#data === null) {
      this.#data = JSON.parse(this.#json)
    }
    return this.#data
  }

  getJson() {
    if (this.#json === null) {
      this.#json = JSON.stringify(this.#data, '', 2)
    }
    return this.#json
  }



  // Equivalent of Java's toString()
  // See https://stackoverflow.com/questions/42886953/whats-the-recommended-way-to-customize-tostring-using-symbol-tostringtag-or-ov
  get [Symbol.toStringTag]() {
    if (this.#json) {
      return "TxData with JSON"
    }
    return "TxData with Object"
  }
  toString() {
    if (this.#json) {
      return `${this.#json}`
    }
    return `${this.#data}`
  }

}

// TxData.prototype.toString = function() {
//   return 'ZEYARP'
// }
