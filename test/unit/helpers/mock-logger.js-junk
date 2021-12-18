/*
 * Remember output to the logger, so we can check it during unit tests.
 *
 * Usage:
 *    const mock = new MockLogger()
 *    ...
 *    t.true(t.context.mocklog.contains('error', 'Some SQL error...'))
 *    const log = mock.entries()
 */
const winston = require('winston')
const Transport = require('winston-transport');

let firstTime = true
let currentMock = null

export default class MockLogger {
  constructor() {
    const self = this
    self.reply = undefined
    self.status = 200
    self.log = [ ]

    // Set up the logger
    currentMock = this
    if (firstTime) {
      const { format } = winston
      winston.add(new SaveLogger({
        format: format.combine(
            // format.splat(),
            // format.json()
        ),
      }))
      firstTime = false
    }
  }

  entries() {
    return this.log
  }

  /**
   * Return true if an log message exists with the specified level, and
   * containing the specified substring.
   *
   * @param {string} level Error level (error, warning, info, etc)
   * @param {string} substr A string to search for in the log messages.
   */
  contains(level, substr) {
    // console.log(`logContains(${level}, ${substr})`);

    // Check each log entry
    for (let i = 0; i < this.log.length; i++) {
      const log = this.log[i]

      // Check each log line in the log entry
      if (log.level === level || level === null) {
        if (log.message.indexOf(substr) >= 0) {
          return true
        }
        if (log.extra) {
          for (let eCnt = 0; eCnt < log.extra.length; eCnt++) {
            const extra = log.extra[eCnt]
            if ((typeof(extra) === 'string') && extra.indexOf(substr) >= 0) {
              return true
            }
          }
        }
      }
    }
    return false
  }
}


/**
 * This class acts as a transport for Winston logger.
 * It receives each call to logger.error() etc, and writes the log messages to
 * the current MockHttp created above. The logContains() function above can be
 * used to check specific error messages exist.
 */
class SaveLogger extends Transport {
  constructor(opts) {
    super(opts);
  }

  log(info, callback) {
    // The extra parameters is an array of the 2nd and subsequent parameters passed to logger.error(), etc.
    const extraParametersToLogger = info[Symbol.for('splat')]
    const logRecord = { level: info.level, message: info.message, extra: extraParametersToLogger }

    if (currentMock) {
      currentMock.log.push(logRecord)
    }

    callback();
  } // log()
}//- SaveLogger