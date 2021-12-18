/*
 * Mock a call to a routing function.
 *
 * Usage:
 *    const mock = new MockHttp({ x: 1, y: 'abc'})
 *    await GET_xyz(mock.req, mock.res)
 *    const { status, reply } = await mock.getReply()
 *
 *    t.is(status, 200)
 *    t.is(reply.user.name, 'Fred Smith')
 *    t.true(t.context.mocklog.contains('error', 'Some SQL error...'))
 */
const mimeTypes = require('mime-types')


let firstTime = true

export default class MockHttp {
  constructor(requestData) {
    const self = this

    self.req = {
      ...requestData,
      // Being used by other modules like GET_userEmail
      // is an alias to req.headers() on express
      // see: https://expressjs.com/en/api.html#req
      get: key => {
        if (requestData && requestData.headers) {
          return requestData.headers[key];
        }
        return undefined;
      },
    }

    // Reply info
    self.status = 200
    self.headers = { }
    self.contentType = null
    self.log = [ ]

    // Possible reply types
    self.data = null
    self.chunks = [ ] // Aggregate reply buffers here
    self.stringReply = '' // Aggregate string reply here

    // Stuff for delayed completion, if the service is returning data via a stream / pipe.
    self.viaPipe = {
      serviceRespondingViaPipe: false,
      reachedEndAlready: false,
      resolvePromise: null,
      rejectPromise: null,
    }

    self.res = {
      send: (arg1, arg2) => {
        // console.log(`res.send() called - `, value);
        if (arg2) {

          // Status, reply
          this.status = arg1
          this.data = arg2
        } else {

          // Just the reply
          this.data = arg1
        }
        return this.res
      },
      status: (code) => {
        this.status = code
        return this.res
      },
      attachment: (filename) => {
        this.contentType = mimeTypes.contentType(filename)
        return this.res
      },
      setHeader: (name, value) => {
        this.headers[name] = value
      },

      /*
       *  Below here simulates a stream, so the service can return using pipe(res).
       */
      on: (event, fn) => {
        // console.log(`on(${event}, some-callback)`)
      },
      once: (...args) => {
        // console.log(`once()`, ...args)
      },
      write: (data) => {
        // console.log(`write() - ${data.length} ${typeof(data)}`, data)
        if (typeof(data) === 'string') {
          this.stringReply += data
        } else {
          this.chunks.push(data)
        }
      },
      end: (data) => {
        // console.log(`end()`)

        // Add any final stuff to the reply.
        if (data) {
          // console.log(`have data ${data.length} ${typeof(data)}`);
          if (typeof(data) === 'string') {
            this.stringReply += data
          } else {
            console.log(`yarpiy 1`);
            this.chunks.push(data)
            console.log(`yarpiy 2`);
          }
        }
        // If we have a resolve function, we can return the promise now. If not,
        // we'll have to let getReply handle the promise resolve.
        if (this.viaPipe.serviceRespondingViaPipe) {
          // The service IS responding using a pipe / stream.
          if (this.viaPipe.resolvePromise) {
            // We have a resolve function, so getReply has already been called, and we can resolve it's promise now.
            // console.log(`AT END - CALLING RESOLVE`);
            const reply = this.stringReply ? this.stringReply : Buffer.concat(this.chunks)
            this.viaPipe.reachedEndAlready = true
            return this.viaPipe.resolvePromise({
              status: this.status,
              reply: reply,
              log: this.log,
              contentType: this.contentType,
              headers: this.headers
            })
          } else {
            // We don't have a resolve function, so getReply has not been called yet. We'll mark this
            // as complete, so getReply knows it can return the response immediately.
            this.viaPipe.reachedEndAlready = true
            // console.log(`AT END, WITHOUT RESOLVE YET()`);
          }
        }
      },
      emit: (event, ...args) => {
        // console.log(`emit(${event})`);
        if (event === 'pipe') {
          // The service is sending it's reply via a pipe.
          // We'll resolve the promise at the end of writing to this stream (on end).
          // console.log(`Have a passthrough`);
          this.viaPipe.serviceRespondingViaPipe = true
        } else {
          // console.log(`emit(${event})`, ...args)
        }

      },
      removeListener: (...args) => {
        console.log(`removeListener()`, ...args)
      },
    };
  }

  /**
   * Return the values collected during the simulated Http call.
   */
  async getReply() {
    return new Promise((resolve, reject) => {
      if (this.viaPipe.serviceRespondingViaPipe) {
        if (!this.viaPipe.reachedEndAlready) {
          // We haven't reach the end of input yet, so remeber the resolve and
          // reject functions so the strea can call them at the end of input.
          // console.log(`SERVICE RESPONDING VIA STREAM - WAITING FOR END`);
          this.viaPipe.resolvePromise = resolve
          this.viaPipe.rejectPromise = reject
          return // It's in the hands of the stream now.
        }
        // console.log(`SERVICE RESPONDING VIA STREAM - AT END ALREADY`);
      } else {
        // console.log(`SERVICE NOT RESPONDING VIA STREAM`);
      }

      // Either the stream has already reached the end of input, or the service is not responding via a stream.
      // console.log(`chuNKS LENGTH IS ${this.chunks.length}`);
      let reply = this.data
      if (this.stringReply) {
        reply = this.stringReply
      } else if (this.chunks.length > 0) {
        reply = Buffer.concat(this.chunks)
      }
      resolve({
        status: this.status,
        reply: reply,
        log: this.log,
        contentType: this.contentType,
        headers: this.headers
      })
    })
  }

  /**
   * Return true if an log message exists with the specified level, and
   * containing the specified substring.
   *
   * @param {string} level Error level (error, warning, info, etc)
   * @param {string} substr A string to search for in the log messages.
   */
  logContains(level, substr) {
    // console.log(`logContains(${level}, ${substr})`);

    // Check each log entry
    for (let i = 0; i < this.log.length; i++) {
      const log = this.log[i]

      // Check each log line in the log entry
      if (log.level === level) {
        for (let eCnt = 0; eCnt < log.extra.length; eCnt++) {
          const extra = log.extra[eCnt]
          if (extra.indexOf(substr) >= 0) {
            return true
          }
        }
      }
    }
    return false
  }
}


// /**
//  * This class acts as a transport for Winston logger.
//  * It receives each call to logger.error() etc, and writes the log messages to
//  * the current MockHttp created above. The logContains() function above can be
//  * used to check specific error messages exist.
//  */
// class SaveLogger extends Transport {
//   constructor(opts) {
//     super(opts);
//   }

//   log(info, callback) {
//     // The extra parameters is an array of the 2nd and subsequent parameters passed to logger.error(), etc.
//     const extraParametersToLogger = info[Symbol.for('splat')]
//     const logRecord = { level: info.level, message: info.message, extra: extraParametersToLogger }

//     if (currentMock) {
//       currentMock.log.push(logRecord)
//     }

//     callback();
//   } // log()
// }//- SaveLogger