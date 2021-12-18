import fs from 'fs'
import path from 'path'
import juice from '@tooltwist/juice-client'

export default {
  confirmMockMode,
  clearMockFiles,
  loadFiles,
  mergeContent,
}

async function confirmMockMode() {
  // Check we have the correct config variables set
  const liveSms = await juice.string('app.liveSms', juice.OPTIONAL)
  const liveEmail = await juice.string('app.liveEmail', juice.OPTIONAL)
  const mockFolder = await juice.string('app.mockFolder', juice.OPTIONAL)

  // console.log(`liveSms=`, liveSms);
  // console.log(`mockSmsFolder=`, mockSmsFolder);
  if ((!liveSms || !liveEmail) && !mockFolder) {
    t.fail('Must define both of app.liveSms and app.liveEmail, or else app.mockSmsFolder')
  }
}

async function clearMockFiles() {
  const dir = await juice.string('app.mockFolder', juice.MANDATORY)

  // Load the files into memory
  const files = fs.readdirSync(dir)
  for (const file of files) {
    fs.unlinkSync(path.join(dir, file))
  }
}

async function loadFiles () {
  const mockSmsFolder = await juice.string('app.mockFolder', juice.MANDATORY)
  const TINY_DELAY = 5 // ms

  // We need a slight delay, to give time for the files to get written to disk.
  return new Promise((resolve, reject) => {
    setTimeout(async() => {
      // Load the files into memory
      const files = fs.readdirSync(mockSmsFolder)
      const jsonFiles = [ ]
      for (let i = 0; i < files.length; i++) {
        const filename = files[i]
        const filePath = `${mockSmsFolder}/${filename}`
        let type = 'unknown'
        if (filename.startsWith('sms')) {
          type = 'sms'
        } else if (filename.startsWith('email')) {
          type = 'email'
        }
        const raw = fs.readFileSync(filePath, 'utf8')
        var obj = JSON.parse(raw)
        jsonFiles.push({
          type,
          name: filename,
          path: filePath,
          data: obj
        })
      }
      return resolve(jsonFiles)
    }, TINY_DELAY)
  })
}

/**
 * Used for checking variables being passed to
 * a Mandrill email template.
 * @param {Array.<object>} mergeVars
 * @param {string} name
 */
function mergeContent(mergeVars, name) {
  for (let i = 0; i < mergeVars.length; i++) {
    const item = mergeVars[i]
    if (item.name === name) {
      return item.content
    }
  }
  return null
}
