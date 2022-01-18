/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

/**
 * While debugging it is sometimes hard to work out the nesting level of
 * pipelines and steps. This function returns a prefix that can be inserted
 * in front of debug messages to provide indenting, and indicating the
 * level within the call hierarchy.
 *
 * @param {number} level Indent level
 * @returns A string to display before messages and debug output
 */
export default function indentPrefix(level) {

  let s = ''
  for (let i = 0; i < level; i++) {
    s += '    '
  }
  s += `${level}  `
  return s
}

