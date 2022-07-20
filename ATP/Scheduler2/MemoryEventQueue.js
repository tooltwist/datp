/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

/**
 * We'll use a memory queue - add to the end and remove from the front.
 * But, to avoid the O(n) cost of using unshift, we'll only shuffle the
 * items forward once half the array has become empty.
 * 
 * Kudos to Matt Timmermans
 * https://stackoverflow.com/questions/68325148/implement-fifo-data-structure-in-javascript-without-array-pop-push-shift-meth
 */
export class MemoryEventQueue {
  #start
  #array

  constructor() {
    this.#start = 0
    this.#array = []
  }

  add(event) {
    // assert(event.type)
    this.#array[this.#array.length] = event
  }

  next() {
    if (this.#start >= this.#array.length) {
      return null;
    }
    const event = this.#array[this.#start++]

    // Perhaps tidy up the queue
    if (this.#start > 100 && this.#start >= this.#array.length - this.#start) {
        //move all the elements into the free space at beginning
        // console.log(`moving from ${this.#start}`)
        let d=0;
        for (let i = this.#start; i < this.#array.length; ++i) {
          this.#array[d++] = this.#array[i];
        }
        this.#start = 0
        this.#array.length = d
    }
    return event
  }

  len () {
    // console.log(`len=> ${this.#array.length} - ${this.#start} = ${this.#array.length - this.#start}`)
    return this.#array.length - this.#start
  }
}
