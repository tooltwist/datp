/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
export default function pad(str, len) {
  if (!str) str = ''
  if (str.length > len) {
    return str.substring(0, len)
  }
  while (str.length < len) {
    str += ' '
  }
  return str
}
