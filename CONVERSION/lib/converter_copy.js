/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
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
  async convert(handler, destObject, toField, source, description, targetType) {
    // console.log(`converter_copy.convert(${toField}, ${source}, ${targetType})`)
    try {
      const value = handler.getSourceValue(source)

      // Special handling for amount3
      if (targetType && targetType === 'amount') {
        if (typeof(value) === 'object' && typeof(value.unscaledAmount) === 'number') {
          // Copy from { currency, unscaledAmount, scale } to number
          let scale = value.scale ? value.scale : 2
          let amount = value.unscaledAmount
          while (scale-- > 0) {
            amount /= 10.0
          }
          // const amount = value.unscaledAmount / scale
          handler.setValue(destObject, toField, amount)
        } else {
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
