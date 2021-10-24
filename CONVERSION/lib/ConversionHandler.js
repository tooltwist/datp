/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import converter_copy from './converter_copy'
import converter_literal from './converter_literal'
import converter_skip from './converter_skip'


export default class ConversionHandler {
  #target

  constructor() {
    this.sources = {} // name -> data
    this.definitions= { }
    this.defaultConverter = new converter_copy()
    this.converters = {
      copy: this.defaultConverter,
      literal: new converter_literal(),
      skip: new converter_skip(),
    }
    this.#target = { }
  }

  addSource(name, definition, object) {
    if (typeof(object) === 'string') {
      object = JSON.parse(object)
    }
    this.sources[name] = object
    this.definitions[name] = definition
  }

  setInitialTarget(target) {
    this.#target = target
  }

  /**
   *
   * @param {*} rules
   * @param {[field]} targetFieldIndex Optional [ fieldpath => { name, type, mandatory } }
   * @returns
   */
  convert(rules, targetFieldIndex) {
    // console.log(`handler.convert()`)
    // console.log(`rules=`, rules)
    // const obj = { }

    for (const rule of rules) {
      // console.log(`  -> ${rule.field} = ${rule.source}`)
      let targetType = null
      if (targetFieldIndex && targetFieldIndex[rule.field]) {
        targetType = targetFieldIndex[rule.field].type
        // console.log(`    targetType=`, targetType)
      }
      const description = `${rule.source} => ${rule.field}`

      // Call the converter
      let converter = this.converters[rule.converter]
      if (!converter) {
        converter = this.defaultConverter
      }
      converter.convert(this, this.#target, rule.field, rule.source, description, targetType)
    }

    return this.#target
  }

  getSourceValue(path) {
    const pos = path.indexOf(':')
    if (pos < 0) {
      throw new Error(`Invalid rule (source=${path}) - value skipped`)
    }
    const sourceName = path.substring(0, pos).trim()
    // console.log(`      sourceName=`, sourceName)
    const fieldName = path.substring(pos + 1).trim()
    // console.log(`      fieldName=`, fieldName)
    const sourceObject = this.sources[sourceName]
    // console.log(`      sourceObject=`, sourceObject)

    if (!sourceObject) {
      throw new Error(`Unknown source (${sourceName})`)
    }

    const value = this.getValue(sourceObject, fieldName)
    // console.log(`      value=`, value)
    return value
  }

  recurseThroughAllFields(sourceName, fn) {
    const sourceObject = this.sources[sourceName]
    // console.log(`      sourceObject=`, sourceObject)
    if (!sourceObject) {
      throw new Error(`Unknown source (${sourceName})`)
    }
    this.recurseThroughAllFieldsRecurse(sourceObject, '', fn)
  }

  recurseThroughAllFieldsRecurse(object, prefix, fn) {
    for (let property in object) {
      const path = `${prefix}${property}`
      const value = object[property]
      if (typeof(value) === 'object') {
        this.recurseThroughAllFieldsRecurse(value, `${path}.`, fn)
      } else {
        fn(path, value)
      }
    }
  }

  getValue(object, field) {
    // console.log(`        - getValue(object, ${field})`, object)
    // console.log(`typeof(object)=`, typeof(object))
    // Should handle arrays
    const pos = field.indexOf('.')
    if (pos < 0) {
      const value = object[field]
      // console.log(`found value=`, value)
      if (typeof(value) === 'undefined') {
        return null
      }
      return value
    } else {
      const subField = field.substring(pos + 1)
      const name = field.substring(0, pos)
      const fieldObj = object[name]
      if (!fieldObj) {
        return null
      }
      return this.getValue(fieldObj, subField)
    }
  }

  setValue(object, path, value) {
    // console.log(`ConversionHandler.setValue(${path}, ${value} (${typeof(value)}))`)

    //ZZZZ Should handle arrays?
    const pos = path.indexOf('.')
    if (pos < 0) {
      // Set object.<path>
      object[path.trim()] = value
    } else {
      // Set object.<prefix>.<suffix>
      const prefix = path.substring(0, pos).trim()
      const suffix = path.substring(pos + 1).trim()
      let nestedObject = object[prefix]
      // See if object.<prefix> already exists
      if (!nestedObject) {
        nestedObject = {}
        // console.log(`NEW OBJECT`)
        object[prefix] = nestedObject
      } else if (typeof(nestedObject) !== 'object') {
        throw new Error(`${path} does not refer to an object`)
      } else {
        // console.log(`EXISTING OBJECT`)
      }
      this.setValue(nestedObject, suffix, value)
    }
  }

}
