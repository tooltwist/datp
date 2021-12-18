// const { google } = require('googleapis');
// import gdrive from '../../../gdrive'
// import GoogleDriveFolders from '../../../GoogleDriveFolders'
// import CredentialsAndOwners from '../../../CredentialsAndOwners'
// import OwnerFolder from '../../../OwnerFolder'
// import mysql from '../../../lib/database-mysql'
// import { Owners } from '../../../misc'
// import colors from 'colors'

// //YARP
// //WAGHAT
// import GAPI_CREDENTIALS_PHIL from '/opt/Development/Projects/mbc/mbc-configs/local-server/volumes/mbc/config/gapi-localmaster-credentials.json'
// const defaultScopes = [
//   'https://www.googleapis.com/auth/documents',
//   'https://www.googleapis.com/auth/documents.readonly',
//   'https://www.googleapis.com/auth/drive',
//   'https://www.googleapis.com/auth/drive.file',
//   'https://www.googleapis.com/auth/drive.readonly',
//   'https://www.googleapis.com/auth/drive.metadata.readonly',
//   'https://www.googleapis.com/auth/drive.appdata',
//   'https://www.googleapis.com/auth/drive.metadata',
//   'https://www.googleapis.com/auth/drive.photos.readonly',
//   'https://www.googleapis.com/auth/presentations',
//   'https://www.googleapis.com/auth/presentations.readonly',
//   'https://www.googleapis.com/auth/spreadsheets',
//   'https://www.googleapis.com/auth/spreadsheets.readonly'
// ]







// let haveFolders = false
// // let drive = null
// // let parentOwner = null
// // let childOwner = null
// // let parentFolderId = null
// // let childFolderId = null


export default {

  async prepareTestFolders() {
    //console.log(`prepareTestFolders()`);

    // if (haveFolders) {
    //   return
    // }

    // // Get Google credentials
    // const credentials = await CredentialsAndOwners.gapiCredentials()
    // const token = await gdrive.getGapiToken(credentials)
    // const drive = google.drive({ version: 'v3', auth: token });

    // const testId = process.pid
    // const parentOwner = `test-unit-${testId}-parent`
    // const childOwner = `test-unit-${testId}-child`
    // const parentFolderId = await GoogleDriveFolders.ownersFolderId(drive, parentOwner)
    // const childFolderId = await GoogleDriveFolders.ownersFolderId(drive, childOwner)

    // haveFolders = true

    // return {
    //   testId,
    //   drive,
    //   childOwner,
    //   childFolderId,
    //   parentOwner,
    //   parentFolderId
    // }
  },

  // /**
  //  *
  //  * @param {boolean} practice
  //  * @param {*} drive
  //  * @param {*} parentOwner
  //  * @param {*} childowner
  //  */
  // async getFolderUpdates(practice, drive, parentOwner, childOwner, approvals) {
  //   const parentFolder = await OwnerFolder.load(drive, parentOwner)
  //   const childFolder = await OwnerFolder.load(drive, childOwner)
  //   await childFolder.mapParents(parentFolder)
  //   // let approvals = null
  //   const changes = await childFolder.checkForUpdates(practice, approvals)

  //   return {
  //     childFolder,
  //     parentFolder,
  //     childFolderId: childFolder.folderId,
  //     parentFolderId: parentFolder.folderId,
  //     changes
  //   }
  // },


  // async createTestFile(drive, folderId, name) {
  //   console.log(`    ** createTestFile(folderId=${folderId}, name=${name})`.gray);

  //   const text = "Yarp!"
  //   const Readable = require('stream').Readable
  //   const stream = new Readable()
  //   stream.push(text)
  //   stream.push(null)      // indicates end-of-file basically - the end of the stream
  //   const media = {
  //     mimeType: 'text/plain',
  //     body: stream //fs.createReadStream('files/photo.jpg')
  //   };

  //   // See https://developers.google.com/drive/api/v3/reference/files/create
  //   const fileMetadata = {
  //     name,
  //     // mimeType: 'application/vnd.google-apps.folder',
  //     parents: [folderId]
  //   };

  //   const response = await drive.files.create({
  //     resource: fileMetadata,
  //     media: media,
  //     fields: 'id'
  //   })
  //   // console.log(`response=`, response);
  //   return response.data.id
  // },


  // async updateTestFile(drive, fileId, text) {
  //   // console.log(`updateTestFile(fileId=${fileId}, text=${text})`);

  //   var Readable = require('stream').Readable
  //   var stream = new Readable()
  //   stream.push(text)
  //   stream.push(null)      // indicates end-of-file basically - the end of the stream

  //   // See https://developers.google.com/drive/api/v3/reference/files/update
  //   // const fileMetadata = {
  //   //   fileId: fileId,
  //   //   // fileId: fileId,
  //   //   // name,
  //   //   // mimeType: 'application/vnd.google-apps.folder',
  //   //   // parents: [ folderId]
  //   // };
  //   const media = {
  //     mimeType: 'text/plain',
  //     body: stream //fs.createReadStream('files/photo.jpg')
  //   };
  //   const response = await drive.files.update({
  //     fileId,
  //     media: media,
  //     fields: 'id,version'
  //   }, { method: 'PATCH' })
  //   // console.log(`response=`, response);
  //   return response.data.version
  // },

  // async hackDsDocument(practice, owner, documentId, newMasterId, newParentId) {
  //   console.log(`    ** hackDsDocument(practice=${practice}, owner=${owner}, documentId=${documentId}, newMasterId=${newMasterId}, newParentId=${newParentId})`.gray);

  //   // Load the existing local-master files from the database.
  //   const db = await mysql.checkConnection()
  //   return new Promise((resolve, reject) => {
  //     let sql = `UPDATE ds_document `
  //     const params = [ ]
  //     let sep = ' SET'
  //     if (newMasterId) {
  //       sql += `${sep} master_document_id=?`
  //       params.push(newMasterId)
  //       sep = ` AND`
  //     }
  //     if (newParentId) {
  //       sql += `${sep} parent_document_id=?`
  //       params.push(newParentId)
  //       sep = ` AND`
  //     }
  //     sql += ` WHERE owner=? AND document_id=?`
  //     params.push(owner)
  //     params.push(documentId)
  //     //  console.log(`sql=${sql}`);
  //     //  console.log(`params=`, params);
  //     db.query(sql, params, async function (err, result) {
  //       if (err) return reject(err);
  //       // console.log(`hackDsDocument result=`, result);
  //       const updated = (result.affectedRows == 1)
  //       return resolve(updated)
  //     })
  //   })
  // },

  // async findMasterId(drive) {
  //   // console.log(`    ** findMasterId()`.gray);

  //   // Choose the id of the first file in the 'master' folder on the Google Drive.
  //   const folderId = await GoogleDriveFolders.ownersFolderId(drive, Owners.OWNER_PRODMASTER, false)
  //   console.log(`Master folderId is ${folderId}`);
  //   const files = await gdrive.getFilesInFolder(drive, folderId)
  //   if (files.length > 0) {
  //     return files[0].id
  //   } else {
  //     throw new Error(`Folder ${folderId} for '${Owner.OWNER_PRODMASTER}' contains no files.`)
  //   }
  // }

}


// /** From https://takamin.github.io/gdrive-fs/jsdoc/gdfs.js.html
//  * Upload a file content to update a existing file.
//  * @param {string} fileId The file id to update.
//  * @param {string} mimeType The content type of the file.
//  * @param {any} data The file content.
//  * @returns {Promise<object>} The response of the API.
//  */
// Gdfs.updateFile = async (fileId, mimeType, data) => {
//   const response = await requestWithAuth("PATCH",
//       "https://www.googleapis.com/upload/drive/v3/files/"+fileId,
//       { uploadType: "media" },
//       { "Content-Type": mimeType },
//       data);
//   return JSON.parse(response);
// };
// /**
//  * @param {string} method The request method.
//  * @param {string} endpoint The endpoint of API.
//  * @param {object} queryParams The query parameters.
//  * @param {object} headers The request headers.
//  * @param {any} body The request body.
//  * @returns {Promise<object>} The response of the request.
//  */
// const requestWithAuth = (method, endpoint, queryParams, headers, body) => {
//   let xhr = new XMLHttpRequest();
//   xhr.open(method, createUrl(endpoint, queryParams), true);
//   headers = headers || {};
//   Object.keys(headers).forEach( name => {
//       xhr.setRequestHeader(name, headers[name]);
//   });
//   xhr.setRequestHeader("Authorization",
//       "Bearer " + getAccessToken());
//   xhr.timeout = 30000;
//   return new Promise( (resolve, reject) => {
//       xhr.onload = () => { resolve(xhr.responseText); };
//       xhr.onerror = () => { reject(new Error(xhr.statusText)); };
//       xhr.ontimeout = () => { reject(new Error("request timeout")); };
//       xhr.send(body);
//   });
// };
