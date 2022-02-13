/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

/**
 * This class implements revolving statistics collection.
 *
 * The user of this class throws values at it, and it will aggregate
 * them into X time periods based on the current time.
 *
 * For example, we can record the number of transaction for every
 * second of the past minute. Or the number of events queued during each
 * five minute period over the past 35 minutes.
 *
 * The class is initialized with the length of each time interval
 * and the number of intervsls to record. The use then calls add() to
 * increment the value for the current time.
 *
 * As time progresses, old values are thrown away, so the number of values
 * record stays constant.
 */
export default class StatsCollector {
  //  #elements is our statistics collection array, containing
  //  #numElements integers, each counting the values we are fed
  //  during a #elementDuration millisecond period.
  //
  // We divide time up into separate intervals of #duration milliseconds,
  // and call them slots. A slot number specifies a time period since
  // epoche (e.g. how many minutes since 1 Jan 1970)
  //
  // #elements is a circular array:
  // The data values are continuosly added, being stored in subsequent array
  // positions according to the slot number. As the insert point moves beyond
  // the end of the array we loop back to the start of the array at position
  // zero. So, data values are added at index [slotNo % #numElements].
  //
  // We record the slotNo of the most recently added data value. If the array
  // index of that value was I, then the data values preceding it (e.g. for each
  // second of the past minute) will be found in positions back to the start of
  // array, and retreating back from teh end of the array to just after the
  // current slotNo:
  //
  //       I-1, I-2... 0, numElements-1, numElements-2... I+1
  //
  // We make the array size on larger than the required number of intervals
  // because when we report, we only want to show completed intervals. There
  // is no point reporting on a time intervl that may be still updating.
  //
  #elements
  #numElements
  #elementDuration

  // Most recently updated slot (the "interval slotNo" since 1970, not the index in the array)
  #mostRecentSlot

  constructor(timeIntervalMs, numIntervals) {
    this.#mostRecentSlot = 0
    this.#numElements = numIntervals + 1
    this.#elements = [ this.numElements ]
    this.#elementDuration = timeIntervalMs
    // console.log(`this.#elements=`, this.#elements)
  }

  add(value) {
    const now = Date.now()
    // console.log(`now=`, now)
    const slot = Math.floor(now / this.#elementDuration)
    // console.log(`slot=`, slot)

    // To update our stats, there are a few possibilities.

    //  Three possible scenarios, based on how long it has been since
    //  we recorded our previous value - it may be necessary to clear out
    //  cells in our array for time periods where nothing happened.
    if (slot === this.#mostRecentSlot) {
      //  1. If this new value is in the same slot as the previously recorded
      //  value, we add it's value to that slot's position in the array.
      // console.log(`In the current slot`)
    } else if (slot < this.#mostRecentSlot + this.#numElements) {
      //  2. If the new slot is less than numElements beyond the previously
      //  recorded value, we clear up to the new slot's position in the array,
      //  wrapping around off the end as required, then save the new value.
      // console.log(`not too late - clear some elements`)
      for (let s = this.#mostRecentSlot + 1; s <= slot; s++) {
        this.#elements[s % this.#numElements] = 0
      }
    } else {
      //  3. If the new slot is more than numElements beyond the previously
      //  recorded value, all the data in the array is before the current recording
      //  period, so we clear the array then save the new value.
      // console.log(`too much - wipe the array`)
      for (let i = 0; i < this.#numElements; i++) {
        this.#elements[i] = 0
      }
      // this.#elements[0] += 123
      // this.#elements[0] += 123
      // console.log(`this.#elements=`, this.#elements)
    }

    // Add to the value for the current time slot
    const index = slot % this.#numElements
    // console.log(`add ${value} to position ${index}`)
    this.#elements[index] += value
    // console.log(`this.#elements=`, this.#elements)

    this.#mostRecentSlot = slot
  }

  dumpArray() {
    // Reset the array up to the current time
    this.add(0)
    // console.log(`this.#elements=`, this.#elements)
    let s = ''
    let sep = 'elements: [ '
    for (const value of this.#elements) {
      s += `${sep}${value}`
      sep = ', '
    }
    s += ' ]'
    console.log(s)
  }

  display(label) {
    // Bring it up to date
    this.add(0)

    // Prepare the string
    let s = ''
    let sep = `${label}: [ `
    const stats = this.getStats()
    for (const value of stats.values) {
      s += `${sep}${value}`
      sep = ', '
    }
    s += ' ]'
    console.log(s)
  }

  getStats() {
    this.add(0)
    // console.log(`this.#elements=`, this.#elements)
    // console.log(`this.#mostRecentSlot=`, this.#mostRecentSlot)
    const i = this.#mostRecentSlot % this.#numElements
    const after = (i < this.#numElements-1) ? this.#elements.slice(i+1, this.#numElements) : [ ]
    const before = (i > 0) ? this.#elements.slice(0, i) : [ ]
    // console.log(`after=`, after)
    // console.log(`before=`, before)
    const values = [...after, ...before]

    const to = new Date(this.#mostRecentSlot * this.#elementDuration)
    const from = new Date((this.#mostRecentSlot - this.#numElements + 1) * this.#elementDuration)
    return {
      from,
      to,
      interval: this.#elementDuration,
      num: values.length,
      values,
    }
  }
}
