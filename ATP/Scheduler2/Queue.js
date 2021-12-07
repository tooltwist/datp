
export class Queue {
  #first // First item in the queue
  #last // Last item in the queue

  constructor () {
    this.#first = null
    this.#last = null
  }

  /**
   * Add an item at the end of the queue
   * @param {QueueItem} item An object that extends QueueItem
   * @returns The item added to the queue
   */
  enqueue(item) {
    // console.log(`ok 1 - enqueue()`)
    item.__setIsInQueue(true)
    item.__setNext(null)
    item.__setPrevious(this.#last)
    // console.log(`ok 2`)

    if (this.#last) {
      // console.log(`ok 3 - not empty`)
      // Non-empty list
      this.#last.__setNext(item)
      this.#last = item
    } else {
      // empty queue
      // console.log(`ok 3 - is empty`)
      this.#last = this.#first = item
    }
    return item
  }

  /**
   * @returns The item at the head of the queue, removed from the queue
   */
  dequeue() {

    if (this.#first) {
      const item = this.#first
      this.#first = item.__getNext()
      if (this.#first === null) {
        this.#last = null
      }
      item.__setIsInQueue(false)
      item.__setPrevious(null)
      item.__setNext(null)
      return item
    }
    return null
  }

  /**
   * Remove an item from wherever it lies in the queue.
   * Note that only basic checking is done that the item is a some queue. If it is not
   * in any queue an exception will be thrown. If it is in another queue, calling this
   * might corrupt that queue.
   *
   * @param {QueueItem} item An item in the queue
   */
  remove(item) {
    item.__setIsInQueue(false)
    const previous = item.__getPrevious()
    const next = item.__getNext()

    if (previous) {
      previous.__setNext(next)
    } else {
      // Must be first in the queue
      this.#first = next
    }

    if (next) {
      next.__setPrevious(previous)
    } else {
      // Must be last in the quue
      this.#last = previous
    }
    item.__setPrevious(null)
    item.__setNext(null)
  }

  getFirst() {
    return this.#first
  }

  getLast() {
    return this.#last
  }

  setFirst(item) {
    this.#first = item
  }

  setLast(item) {
    this.#last = item
  }

  size() {
    let size = 0
    for (let item = this.#first; item !== null; item = item.__getNext()) {
      size++
    }
    return size
  }

  dump() {
    for (let item = this.#first; item !== null; item = item.__getNext()) {
      console.log(`item=`, item)
    }
  }

  // Return a generator
  * items() {
    for (let item = this.#first; item !== null; item = item.__getNext()) {
      yield item
    }
  }
}


// console.log(`here we go...`)
// const q = new Queue()

// const j1 = new QueueItem('1')
// const j2 = new QueueItem('2')
// const j3 = new QueueItem('3')
// const j4 = new QueueItem('4')


// console.log(`\nA:`)
// q.enqueue(j1)
// q.dump()

// console.log(`\nB:`)
// q.enqueue(j2)
// q.dump()

// console.log(`\nC:`)
// q.enqueue(j3)
// q.enqueue(j4)
// q.dump()

// console.log(`\nD:`)
// q.remove(j2)
// q.dump()

// console.log(`\nE:`)
// q.remove(j3)
// q.dump()

// console.log(`\nF:`)
// q.enqueue(j2)
// q.enqueue(j3)
// q.dump()

// console.log(`\nG:`)
// try {
//   q.enqueue(j2)
// } catch (e) {
//   console.log(`e=`, e)
// }
// q.dump()


// console.log(`\nH:`)
// q.remove(j3)
// try {
//   q.remove(j3)
// } catch (e) {
//   console.log(`e=`, e)
// }
// q.dump()



