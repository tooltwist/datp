/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */


export default class {

  /**
   *
   * @param {*} handler
   * @param {*} destObject
   * @param {*} toField
   * @param {*} source
   * @param {*} description
   * @param {String} targetType Optional type for the destination field.
   */
  async convert(instance, handler, destObject, toField, source, description, targetType) {
    // console.log(`converter_copy.convert(${toField}, ${source}, ${targetType})`)
    try {
      const value = handler.getSourceValue(source)
      // instance.debug(`     COPY ${source} -> ${toField}  (${value})`)

      // Special handling for amount3
      if (targetType && targetType === 'amount') {
        // instance.debug(`     - target is amount`)
        if (typeof(value) === 'object' && typeof(value.unscaledAmount) === 'number') {
          // instance.debug(`     - have value.unscaledAmount`)
          // Copy from { currency, unscaledAmount, scale } to number
          let scale = value.scale ? value.scale : 2
          let amount = value.unscaledAmount
          while (scale-- > 0) {
            amount /= 10.0
          }
          // const amount = value.unscaledAmount / scale
          // instance.debug(`     - convert from amount3 to number ${amount}`)
          handler.setValue(destObject, toField, amount)
        } else {
          // instance.debug(`     - do NOT have value.unscaledAmount`)
          handler.setValue(destObject, toField, value)
        }
      } else {
        // Regular field
        handler.setValue(destObject, toField, value)
      }
    } catch (e) {
      const msg = `Rule failed: ${description} (${e.message})`
      console.log(msg)
      throw new Error(msg)
    }
  }
}
