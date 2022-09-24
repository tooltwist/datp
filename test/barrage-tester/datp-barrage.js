/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
const csi = require('ansi-escape')
const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');
const crypto = require('crypto')

// See https://github.com/mcollina/autocannon
// See https://www.nearform.com/blog/load-testing-with-autocannon/
const autocannon = require('autocannon')



const app = express();

const PROMPT = 'Command (h for help): '

const STATUS_NOT_STARTED = 0
const STATUS_ALLOCATED = 1
const STATUS_RUNNING = 2
const STATUS_1 = 3
const STATUS_2 = 4
const STATUS_3 = 5
const STATUS_SLEEPING = 6
const STATUS_COMPLETE = 7
const STATUS_ERROR = 8
const STATUS_UNKNOWN = 8

module.exports.BarrageTester = class BarrageTester {
  #autocannonDefinition
  #numTests
  #tests // [ { started, status }, ...]
  #columns
  // #users
  // #running
  #externalIdIndex // Map externalId => testNo
  #badStatusCounter

  constructor(options) {
    this.#autocannonDefinition = null
    this.#tests = [ ]
    this.#columns = options.columns ? options.columns : 100
    // this.#running = false
    this.#externalIdIndex = new Map()
    this.#badStatusCounter = 0

    this.#numTests

    this.status = {
      STATUS_NOT_STARTED,
      STATUS_ALLOCATED,
      STATUS_RUNNING,
      STATUS_1,
      STATUS_2,
      STATUS_3,
      STATUS_SLEEPING,
      STATUS_COMPLETE,
      STATUS_ERROR,
      STATUS_UNKNOWN
    }
  }

  run(autocannonDefinition) {
    console.log(`run()`)
    this.#autocannonDefinition = autocannonDefinition
    if (!autocannonDefinition.amount) {
      autocannonDefinition.amount = 500
    }
    this.#numTests = autocannonDefinition.amount
    // console.log(`this.#numTests=`, this.#numTests)
    this.initializeTests()

    // Start the CLI
    this.clearPage()
    this.display()
    this.cli()

    this.startWebhookServer()
  }
  
  startWebhookServer() {

    // Start the express server used for webhooks
    const port = 3000

    // We don't care where the webhook comes from.
    app.use(cors());

    // Configuring body parser middleware
    app.use(bodyParser.urlencoded({ extended: false }));
    app.use(bodyParser.json());
    const me = this

    app.post('/webhook', (req, res) => {
      // console.log(`POST req.body=`, req.body)
      if (!req.body) {
        res.setStatus(500)
        res.send('Missing req.body')
      }
      if (!req.body.metadata) {
        res.setStatus(500)
        res.send('Missing req.body.metadata')
      }
      if (!req.body.metadata.externalId) {
        res.setStatus(500)
        res.send('Missing req.body.metadata.externalId')
      }
      const externalId = req.body.metadata.externalId
      me.setStatusByExternalId(externalId, STATUS_COMPLETE)
      res.send('POST Webhook completed')
    });
    app.listen(port)
  }

  initializeTests() {
    this.#tests = [ ]
    for (let i = 0; i < this.#numTests; i++) {
      this.#tests.push({
        // started: Date.currentTimeMillis(),
        status: STATUS_NOT_STARTED
      })
      this.generateExternalId(i)
    }
  }

  clearPage() {
    console.log(csi.eraseDisplayEntire.escape(''))
  }

  generateExternalId(testNo) {
    for ( ; ; ) {
      // See https://stackoverflow.com/questions/9407892/how-to-generate-random-sha1-hash-to-use-as-id-in-node-js/14869745
      const externalId = crypto.randomBytes(20).toString('hex');

      // See if this externalId is already used (very very unlikely, but we'll check)
      if (!this.#externalIdIndex.has(externalId)) {
        this.setExternalId(testNo, externalId)
        return externalId
      }
    }
  }

  setExternalId(testNo, externalId) {
    if (testNo < 0 || testNo >= this.#numTests) {
      return null
    }
    const test = this.#tests[testNo]

    // If an externalId is already set, unmap it.
    if (test.externalId) {
      this.#externalIdIndex.delete(test.externalId)
    }
    test.externalId = externalId
    // console.log(`#externalIdIndex=`, this.#externalIdIndex)
    this.#externalIdIndex.set(externalId, testNo)
    return externalId
  }

  getExternalId(testNo) {
    if (testNo < 0 || testNo >= this.#numTests) {
      return null
    }
    const test = this.#tests[testNo]
    if (test.externalId === undefined) {
      return null
    }
    return test.externalId
  }

  getTestForExternalId(externalId) {
    const testNo = this.#externalIdIndex.get(externalId)
    if (testNo === undefined) {
      return -1
    }
    return testNo
  }

  display() {
    // console.log(
    //   csi
    //     // underline text
    //     // .underline
     
    //     // red text
    //     // .blue
     
    //     // move cursor one line up
    //     // .cursorUp
    //     // .cursorUp
    //     // .cursorUp
    //     // .eraseDisplayEntire
     
    //     // move cursor 2 cells forward
    //     // .cursorForward(2)
    //     // .cursorPosition(y, x)
     
    //     // the text to be escaped
    //     .escape('')
    // )
    for (let i = 0; i < this.#numTests; i++) {
      this.displayTest(i)
    }
    this.cursorToBottom()
    console.log(`  transactions: ${this.#autocannonDefinition.amount}`)
    console.log(`   connections: ${this.#autocannonDefinition.connections}`)
    console.log(`           url: ${this.#autocannonDefinition.url}`)

    // this.cursorToTop()
  }//- display all tests

  displayTest(i) {
    if (i < 0 || i >= this.#numTests) {
      return
    }
    const x = (i % this.#columns) + 4
    const y = Math.floor(i / this.#columns) + 3
    const test = this.#tests[i]
    switch(test.status) {
      case STATUS_NOT_STARTED:
        console.log(csi.cursorPosition(y, x).bgBlack.white.dim.escape('.'))
        break

      case STATUS_ALLOCATED:
        console.log(csi.cursorPosition(y, x).blue.escape('-'))
        break

      case STATUS_RUNNING:
        console.log(csi.cursorPosition(y, x).blue.escape('R'))
        break
      
      case STATUS_1:
        console.log(csi.cursorPosition(y, x).blue.escape('1'))
        break

      case STATUS_2:
        console.log(csi.cursorPosition(y, x).magenta.escape('2'))
        break

      case STATUS_3:
        console.log(csi.cursorPosition(y, x).cyan.escape('3'))
        break

      case STATUS_SLEEPING:
        // console.log(csi.cursorPosition(y, x).bgCyan.escape('✔'))
        console.log(csi.cursorPosition(y, x).bgBlack.white.dim.escape('z'))
        break

      case STATUS_COMPLETE:
        // console.log(csi.cursorPosition(y, x).bgCyan.escape('✔'))
        console.log(csi.cursorPosition(y, x).bgGreen.white.escape('✔'))
        break
      
      case STATUS_ERROR:
        // console.log(csi.cursorPosition(y, x).bgRed.white.bold.escape('✖'))
        // console.log(csi.cursorPosition(y, x).bgRed.white.bold.escape('✖'))
        console.log(csi.cursorPosition(y, x).red.escape('✖'))
        break
    
      default:
        // See https://www.npmjs.com/package/ansi-escape
        console.log(csi.cursorPosition(y, x).bold.escape('?'))
        break
    }

  }

  setStatus(testNo, status) {
    if (testNo >= 0 && testNo < this.#numTests) {
      this.#tests[testNo].status = status
      this.displayTest(testNo)
      this.cursorToTop()
    }
  }

  setStatusByExternalId(externalId, status) {
    // console.log(`setStatusByExternalId(${externalId}, ${status})`)
    const testNo = this.getTestForExternalId(externalId)
    if (testNo < 0) {
      return false
    }
    this.#tests[testNo].status = status
    this.displayTest(testNo)
    this.cursorToTop()
    return true
  }

  cmd_help() {
    this.cursorToBottom()
    console.log(`${csi.bold.escape('Commands')}`)
    console.log(`  ${csi.bold.escape('c <number>')}   - set the number of columns`)
    console.log(`  ${csi.bold.escape('t <number>')}   - set the number of tests`)
    console.log(`  ${csi.bold.escape('s')}            - start the barrage test`)
    console.log(`  ${csi.bold.escape('r')}            - redraw the screen`)
    console.log(`  ${csi.bold.escape('q')}            - quit`)
  }

  cmd_numColumns(cmd) {
    const args = cmd.substring(1).trim()
    const num = parseInt(args)
    if (isNaN(num)) {
      console.log(`usage: ` + csi.bold.escape('c <number>'))
    } else if (num < 5 || num > 500) {
      console.log(`Invalid number of columns`)
    } else {
      this.clearPage()
      this.#columns = num
    }
  }

  cmd_numTests(cmd) {
    const args = cmd.substring(1).trim()
    const num = parseInt(args)
    if (isNaN(num)) {
      console.log(`usage: ` + csi.bold.escape('t <number>'))
    } else if (num <= 0 || num > 1000000) {
      console.log(`Invalid number of tests`)
    } else {
      this.clearPage()
      this.#numTests = num
      this.#autocannonDefinition.amount = num
      this.initializeTests()
    }
  }

  cmd_startTesting() {
    this.initializeTests()
    this.#badStatusCounter = 0
    this.cursorToBottom()

    // Use autocannon to start the transactions
    const instance = autocannon(this.#autocannonDefinition, (err, result) => this.handleAutocannonResult(err, result))  
    instance.on('reqError', (aaa) => {
      console.log(`reqError=`, aaa)
    })
    // autocannon.track(instance, {renderProgressBar: true})
  }

  cursorToBottom() {
    const y = Math.floor(this.#numTests / this.#columns) + 4
    console.log(csi.cursorPosition(y, 1).eraseDisplayEnd.escape(''))
    // console.log(`y=`, y)
  }

  cursorToTop() {
    // process.stdout.write(csi.cursorPosition(1, PROMPT.length).escape(''))
    process.stdout.write(this.prompt())
  }


  prompt() {
    return csi
        // underline text
        // .underline
      
        // red text
        .red
        .bold
      
        // move cursor one line up
        // .cursorUp
        // .cursorUp
        // .cursorUp
        // .eraseDisplayEntire
      
        // move cursor 2 cells forward
        // .cursorForward(2)
        .cursorPosition(1, 1)
        .eraseLineEntire
      
        // the text to be escaped
        .escape(PROMPT)
    // )
  }//- prompt

  async cli() {
    var readline = require('readline');
    const rl = readline.createInterface(process.stdin, process.stdout);
    rl.setPrompt(this.prompt());
    rl.prompt();
    rl.on('line', (cmd) => {
      cmd = cmd.trim().toLowerCase()
      if (cmd === 'q') {
        console.log(csi.eraseDisplayEntire.escape('Bye!'))
        console.log(``)
        process.exit(0)
      } else if (cmd.startsWith('h')) {

        // Show help info
        this.cmd_help()
        this.cursorToTop()
        return
      } else if (cmd.startsWith('c')) {

        // Set the number of columns
        this.cmd_numColumns(cmd)
      } else if (cmd.startsWith('t')) {

        // Set the number of tests
        this.cmd_numTests(cmd)
      } else if (cmd.startsWith('r')) {

        // Redisplay the page
        this.clearPage()
        this.display()
      } else if (cmd.startsWith('s')) {

        // Run tests
        this.cmd_startTesting(cmd)
      } else {
        console.log(`Unknown command.`)
      }

      // rl.close();
      this.display()
      rl.prompt();
    })//- on line
  }// cli

  giveMeATest() {
    // Find a random test waiting to be used
    const start = Math.floor(Math.random() * this.#numTests)
    for (let i = 0; i < this.#numTests; i++) {
      const testNo = (start + i) % this.#numTests
      if (this.#tests[testNo].status === STATUS_NOT_STARTED) {
        this.setStatus(testNo, STATUS_ALLOCATED)
        return this.getExternalId(testNo)
      }
    }
    console.log(`Run out of tests!`)
    return -1
  }

  handleDatpResponse(status, body, context) {
    // console.log(`handleDatpResponse()`)
    // console.log(`status=`, status)
    // console.log(`body=`, body)
    if (status === 200) {
      const reply = JSON.parse(body)

      // Check the request status (not the transaction status)
      if (reply && reply.metadata) {
        context.txId = reply.metadata.txId

        switch (reply.metadata.status) {
          case 'running':
            this.setStatusByExternalId(context.externalId, STATUS_RUNNING)
            break

          case 'success':
            this.setStatusByExternalId(context.externalId, STATUS_COMPLETE)
            break

          case 'sleeping':
            this.setStatusByExternalId(context.externalId, STATUS_SLEEPING)
            break

          default:
            this.setStatusByExternalId(context.externalId, STATUS_UNKNOWN)
            break
        }
      } else {
        console.log(`ERROR STARTING TRANSACTION:`)
        console.log(`reply is null`)
        console.log(`body=`, body)
        this.setStatusByExternalId(context.externalId, STATUS_ERROR)
        this.#badStatusCounter++
      // } else {
        // console.log(`reply=`, reply)
          // console.log(`context=`, context)
        // console.log(`context.externalId=`, context.externalId)
        // console.log(`\n\n\ntester.STATUS_COMPLETE)=`, STATUS_COMPLETE)
        // console.log(`tester=`, tester)
        // this.setStatusByExternalId(context.externalId, STATUS_COMPLETE)
      }
    } else {
      // console.log(`ERROR GETTING STATUS:`)
      console.log(`status=`, status)
      console.log(`body=`, body)
      console.log(`context=`, context)
      console.log(`context.externalId=`, context.externalId)
      this.setStatusByExternalId(context.externalId, STATUS_ERROR)
    }
    // this.cursorToBottom()
    this.cursorToTop()
  }

  handleAutocannonResult(err, result) {
    // console.log(`handleAutocannonResult()`)
    this.cursorToBottom()
    if (err) {
      console.log(`ERROR=`, ERROR)
      console.log(`err=`, err)
    } else {
      // console.log(`\n\nCOMPLETE`)
      // console.log(`result=`, result)
      // autocannon.printResult(result)

      // if (badStatusCounter !== 0) {
        // console.log(``)
        console.log(`${this.#numTests} transactions started`)
        console.log(`${this.#badStatusCounter} INCORRECT STATUSES`)
      // }
    }
    this.cursorToTop()
  }//- handleAutocannonResult

}
 
// console.log('1'.repeat(8))
// console.log(
//   csi
//     // underline text
//     .underline
 
//     // red text
//     .blue
 
//     // move cursor one line up
//     // .cursorUp
//     // .cursorUp
//     // .cursorUp
//     .eraseDisplayEntire
 
//     // move cursor 2 cells forward
//     // .cursorForward(2)
//     .cursorPosition(10, 10)
 
//     // the text to be escaped
//     .escape('2'.repeat(4))
// )

// console.log(``)
// console.log(``)
// console.log(``)
// console.log(`DONE`)

// let s = csi.eraseDisplayEntire()
// console.log(s)
 

