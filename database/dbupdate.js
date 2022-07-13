/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
// import mysql2 from 'mysql2/promise';
import juice from '@tooltwist/juice-client'

const VERBOSE = 0
const SHOW_PROGRESS = false
const SHOW_CONNECTION_DETAILS = false

let promisePool = null
let queryCounter = 0
let connectionCounter = 0

async function checkPool() {
  if (promisePool) {
    return
  }

  try {
    const mysql = require('mysql2');

    // Perhaps show a debugging message
    if (SHOW_CONNECTION_DETAILS) {
      const host = await juice.string('db.host', juice.MANDATORY)
      const port = await juice.integer('db.port', juice.MANDATORY)
      const database = await juice.string('db.database', juice.MANDATORY)
      let user = await juice.string('db.user', juice.MANDATORY)
      let password = await juice.string('db.password', juice.MANDATORY)
      if (user && user.length > 2) {
        user = user.substring(0, 2) + '?????'
      } else {
        user = 'xx?????'
      }
      if (password && password.length > 2) {
        password = password.substring(0, 2) + '?????'
      } else {
        password = 'xx?????'
      }
      console.log(`Connecting to DB: ${host}:${port}, ${database}, ${user}/${password}`)
    }


    // See https://github.com/sidorares/node-mysql2/issues/840
    const pool = mysql.createPool({
      host: await juice.string('db.writehost', juice.MANDATORY),
      user: await juice.string('db.user', juice.MANDATORY),
      password: await juice.string('db.password', juice.MANDATORY),
      port: await juice.integer('db.port', juice.MANDATORY),
      database: await juice.string('db.database', juice.MANDATORY),
      // The maximum number of connections to create at once. (Default: 10)
      connectionLimit: await juice.integer('db.connectionLimit', 10),
      // Determines the pool's action when no connections are available and the limit
      // has been reached. If true, the pool will queue the connection request and call
      // it when one becomes available. If false, the pool will immediately call back
      // with an error. (Default: true)
      waitForConnections: true,
      // The maximum number of connection requests the pool will queue before returning
      // an error from getConnection. If set to 0, there is no limit to the number of
      // queued connection requests. (Default: 0)
      // See https://www.w3resource.com/node.js/nodejs-mysql.php
      queueLimit: 0,
      // Allow multiple mysql statements per query. Be careful with this, it exposes you
      // to SQL injection attacks. (Default: false)
      multipleStatements: false,
      // Generates stack traces on Error to include call site of library entrance ("long stack traces").
      // Slight performance penalty for most calls. Default is true.
      trace: true,
    });

    // During post, the object fields are used as column names.
    // See https://github.com/sidorares/node-mysql2/pull/369
    pool.config.namedPlaceholders = true
    pool.config.connectionConfig.namedPlaceholders = true
    promisePool = pool.promise();

    connectionCounter++
    if (VERBOSE) {
      console.log(`Opened DATP database connection pool #${connectionCounter}.`)
    }


  } catch (error) {
    console.log(`Could not connect - ${error}`);
    // return
    throw error
  }
}

export default async function dbupdate(dbQuery, params = []) {
  const counter = ++queryCounter
  if (VERBOSE > 1) console.log(`----------------------------`)
  if (VERBOSE > 1) {
    console.log(`dbupdate() ${counter}:\n${dbQuery}`)
  // if (VERBOSE) {
    // console.log('dbupdate: ', dbQuery);
    console.log('params: ', params);
  }

  // if (SHOW_PROGRESS) {
  //   console.log(`dbupdate 1 - ${dbQuery}`)
  // }

  // Check we have no undefined bind parameters, because they create
  // a nasty exception that somehow doesn't get caught by the catch here.
  for (let i = 0; i < params.length; i++) {
    if (params[i] === undefined) {
      throw new Error(`Bind parameter must not be undefined [index=${i}]`)
    }
  }

  if (SHOW_PROGRESS) {
    console.log(`dbupdate (${counter}) 2`)
  }

  await checkPool()

  if (SHOW_PROGRESS) {
    console.log(`dbupdate (${counter}) 3`)
  }

  // See https://www.npmjs.com/package/mysql2#using-promise-wrapper
  // create the connection
  // const connection = await mysql2.createConnection(config);
  // query database
  // const [rows, fields] = await connection.execute(dbQuery, params);
  const before = new Date().getMilliseconds()
  // const [rows, fields] = await promisePool.execute(dbQuery, params);
  const [rows, fields] = await promisePool.query(dbQuery, params);
  if (VERBOSE) {
    const after = new Date().getMilliseconds()
    console.log(`dbupdate (${counter}): ${after-before}ms`)
  }

  if (SHOW_PROGRESS) {
    console.log(`dbupdate (${counter}) 4`)
  }

  return rows
}

module.exports = dbupdate;