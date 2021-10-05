
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
