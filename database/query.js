/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
// import mysql2 from 'mysql2/promise';
import juice from '@tooltwist/juice-client'

const VERBOSE = false
const SHOW_PROGRESS = false
const SHOW_CONNECTION_DETAILS = false

let promisePool = null

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
      host: await juice.string('db.host', juice.MANDATORY),
      user: await juice.string('db.user', juice.MANDATORY),
      password: await juice.string('db.password', juice.MANDATORY),
      port: await juice.integer('db.port', juice.MANDATORY),
      database: await juice.string('db.database', juice.MANDATORY),
      connectionLimit: 10,
      waitForConnections: true,
      queueLimit: 0,
      multipleStatements: true,
    });

    // During post, the object fields are used as column names.
    // See https://github.com/sidorares/node-mysql2/pull/369
    pool.config.namedPlaceholders = true
    pool.config.connectionConfig.namedPlaceholders = true

    promisePool = pool.promise();
  } catch (error) {
    console.log(`Could not connect - ${error}`);
    // return
    throw error
  }
}

export default async function query(dbQuery, params = []) {
  if (VERBOSE) {
    console.log('query: ', dbQuery);
    console.log('params: ', params);
  }

  if (SHOW_PROGRESS) {
    console.log(`query 1 - ${dbQuery}`)
  }

  // Check we have no undefined bind parameters, because they create
  // a nasty exception that somehow doesn't get caught by the catch here.
  for (let i = 0; i < params.length; i++) {
    if (params[i] === undefined) {
      throw new Error(`Bind parameter must not be undefined [index=${i}]`)
    }
  }

  if (SHOW_PROGRESS) {
    console.log(`query 2`)
  }

  await checkPool()

  if (SHOW_PROGRESS) {
    console.log(`query 3`)
  }

  // See https://www.npmjs.com/package/mysql2#using-promise-wrapper
  // create the connection
  // const connection = await mysql2.createConnection(config);
  // query database
  // const [rows, fields] = await connection.execute(dbQuery, params);
  const [rows, fields] = await promisePool.execute(dbQuery, params);

  if (SHOW_PROGRESS) {
    console.log(`query 4`)
  }

  return rows
}

module.exports = query;