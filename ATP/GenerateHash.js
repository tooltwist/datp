/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import crypto from 'crypto'

export default function (prefix) {
  // See https://stackoverflow.com/questions/9407892/how-to-generate-random-sha1-hash-to-use-as-id-in-node-js/14869745
  const id = crypto.randomBytes(20).toString('hex');
  const hash = `${prefix}-${id}`
  return hash
}
