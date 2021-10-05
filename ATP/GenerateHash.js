import crypto from 'crypto'

export default function (prefix) {
  // See https://stackoverflow.com/questions/9407892/how-to-generate-random-sha1-hash-to-use-as-id-in-node-js/14869745
  const id = crypto.randomBytes(20).toString('hex');
  const hash = `${prefix}-${id}`
  return hash
}
