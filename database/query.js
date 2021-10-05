import mysql2 from 'mysql2/promise';
import juice from '@tooltwist/juice-client'
// require('mysql2/promise')

const VERBOSE = false

let promisePool = null

async function checkPool() {
  if (promisePool) {
    return
  }

  try {
    const mysql = require('mysql2');

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
    return console.log(`Could not connect - ${error}`);
  }
}

export default async function query(dbQuery, params = []) {
  if (VERBOSE) {
    console.log('query: ', dbQuery);
    console.log('params: ', params);
  }

  await checkPool()

  // See https://www.npmjs.com/package/mysql2#using-promise-wrapper
  // create the connection
  // const connection = await mysql2.createConnection(config);
  // query database
  // const [rows, fields] = await connection.execute(dbQuery, params);

  const [rows, fields] = await promisePool.execute(dbQuery, params);

  return rows
};

module.exports = query;