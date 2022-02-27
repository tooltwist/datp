/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import GenerateHash from "./GenerateHash"

export default class XData {
  #data
  #json

  constructor(initialData) {
    // console.log(`XData.constructor()`, initialData)
    // this.#id = GenerateHash('tx')
    if (initialData === null) {
      initialData = { }
    }
    if (initialData instanceof XData) {
      // console.log(`YARP cloning XData`)
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
        throw new Error(`XData.constructor() must be passed JSON or an Object.`)
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
      return "XData with JSON"
    }
    return "XData with Object"
  }
  toString() {
    if (this.#json) {
      return `${this.#json}`
    }
    return `${this.#data}`
  }

}

/**
 * This function ges passed either an XData or an Object, and
 * returns an Javascrip object. This allows data to be passed around in
 * either format, and then converted to a Javascript object only
 * when required.
 * 
 * @param {XData|Object} xdataOrObject A reference to data.
 * @param {string} errorMessage Message if an exception is thrown
 * @returns Object
 */
export function dataFromXDataOrObject (xdataOrObject, errorMessage) {
  if (xdataOrObject instanceof XData) {
    return xdataOrObject.getData()
  } else if (typeof(xdataOrObject) === 'object') {
    return xdataOrObject
  } else {
    throw new Error(errorMessage)
  }

}

// XData.prototype.toString = function() {
//   return 'ZEYARP'
// }
