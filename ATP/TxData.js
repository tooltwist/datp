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

}