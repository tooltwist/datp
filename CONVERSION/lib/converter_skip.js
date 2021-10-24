/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */


export default class {
  async convert(handler, destObject, toField, source, description) {
    // console.log(`converter_unknown.convert(${toField}, ${source})`)
    console.log(`converter: skipping ${toField}.`)
    const value = 'SKIP'
    handler.setValue(destObject, toField, value)
  }
}
