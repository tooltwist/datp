/**
 * Pause execution for a while.
 * NOTE: Make sure you use 'await pause()'
 *
 * @param {integer} milliseconds
 * @returns null
 */
export default async function pause(milliseconds) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve(null)
    }, milliseconds)
  })
}