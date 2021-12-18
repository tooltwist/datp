
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

