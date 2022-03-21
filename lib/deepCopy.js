/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
const VERBOSE = 0

export function deepCopy(from, to) {
  if (VERBOSE) console.log(`deepCopy()   ${JSON.stringify(from)}  =>  ${JSON.stringify(to)}`)
  if (!to) to = { }
  for (let name in from) {
    const value = from[name]

    // Perhaps delete the value?
    if (name.startsWith('-')) {
      name = name.substring(1)
      delete to[name]
      continue
    }

    // For an object, perhaps replace the entire object.
    // This will lose any existing values in the object.
    let replaceObject = false
    if (name.startsWith('!')) {
      name = name.substring(1)
      replaceObject = true
    }


    if (Array.isArray(value)) {
      // We don't try to merge arrays
      to[name] = cloneArray(value)
      continue
    }

    // Nope, setting the value
    // console.log(`-> ${name}=${value}   (${typeof value})`)
    const type = typeof(value)
    switch (type) {
      case 'string':
      case 'number':
      case 'boolean':
        to[name] = value
        break
      case 'object':
        if (value === null) {
          to[name] = null
        } else {
          let nested = to[name]
          if (!nested || replaceObject) {
            nested = { }
            to[name] = nested
          }
          deepCopy(value, nested)
        }
        break
      case 'undefined':
        // Ignore this value, as does JSON.stringify()
        //ZZZ Write this to the log file
        console.log(`WARNING deepCopy ignoring field with value 'undefined' [${name}]`.magenta)
        console.log(new Error(`deepCopy ignoring field with value 'undefined'  [${name}]`).stack)
        break
      default:
        console.log(`deepCopy: Unknown type [${type}] for ${name}`)
        throw new Error(`Transaction.deepCopy(): unknown data type ${type}`)
    }
  }
  return to
}

export function cloneArray(arr) {
  const newArr = [ ]
  for (const elem of arr) {

    if (Array.isArray(elem)) {
      newArr.push(cloneArray(elem))
      continue
    }

    switch (typeof(elem)) {
      case 'string':
      case 'number':
        newArr.push(elem)
        break

      case 'object':
        const newElem = {}
        deepCopy(newElem, elem)
        newArr.push(elem)
        break

      case 'undefined':
        break

      default:
        console.log(`cloneArray: Unknown element type [${type}]`)
        throw new Error(`Transaction.cloneArray(): unknown data type ${type}`)
    }
  }
  return newArr
}
