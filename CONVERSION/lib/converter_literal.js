/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

export default class {

  async convert(instance, handler, destObject, toField, source, description) {
    // console.log(`converter_literal.convert(${toField}, ${source})`)
    instance.debug(`  LITERAL ${source} -> ${toField}`)
    const value = source
    handler.setValue(destObject, toField, value)
  }

}
