/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors';

const controls = [ ] // id -> object
let initializationError = false
// let initialized = false

function registerControl(id, type, channel, min, max, initialValue = 0) {
  controls[id] = {
    id,
    type,
    channel,
    min,
    max,
    value: initialValue
  }
  // console.log(`controls=`, controls)
}

function setValue(type, channel, value) {
  // console.log(`setValue(${type}, ${channel}, ${value})`)
  // Find the control
  for (const id in controls) {
    // console.log(`id=`, id)
    const ctrl = controls[id]
    if (ctrl.type === type && ctrl.channel === channel) {
      if (value > ctrl.max) {
        value = ctrl.max
      }
      if (value < ctrl.min) {
        value = ctrl.min
      }
      ctrl.value = value
      // console.log(`YARP ${ctrl.id} - ${ctrl.value}`)
      return
    }
  }
}

export async function monitorMidi() {
  // console.log(`monitorMidi()`)


  registerControl('slider0', 'pitch', 0, 1, 16383)
  registerControl('slider1', 'pitch', 1, 1, 16383)
  registerControl('slider2', 'pitch', 2, 1, 16383)
  registerControl('slider3', 'pitch', 3, 1, 16383)
  registerControl('slider4', 'pitch', 4, 1, 16383)
  registerControl('slider5', 'pitch', 5, 1, 16383)
  registerControl('slider6', 'pitch', 6, 1, 16383)
  registerControl('slider7', 'pitch', 7, 1, 16383)

  try {

    const easymidi = require('easymidi');

    const INPUT_NAME = 'nanoKONTROL Studio Bluetooth';

    const input = new easymidi.Input(INPUT_NAME);

    // input.on('noteoff', msg => console.log('noteoff', msg.note, msg.velocity, msg.channel));

    // input.on('noteon', msg => console.log('noteon', msg.note, msg.velocity, msg.channel));

    // input.on('poly aftertouch', msg => console.log('poly aftertouch', msg.note, msg.pressure, msg.channel));
    // input.on('cc', msg => console.log('cc', msg.controller, msg.value, msg.channel));
    // input.on('program', msg => console.log('program', msg.number, msg.channel));
    // input.on('channel aftertouch', msg => console.log('channel aftertouch', msg.pressure, msg.channel));

    input.on('pitch', msg => {
      // console.log('pitch', msg.value, msg.channel)
      setValue('pitch', msg.channel, msg.value)
    })

    // input.on('position', msg => console.log('position', msg.value));
    // input.on('select', msg => console.log('select', msg.song));
    // input.on('clock', () => console.log('clock'));
    // input.on('start', () => console.log('start'))
    // input.on('continue', () => console.log('continue'));
    // input.on('stop', () => console.log('stop'));
    // input.on('reset', () => console.log('reset'));

  } catch (e) {
    console.log(`Midi could not be set up:`, e)
    initializationError = true
  }
}

export async function getMidiValuesV1(req, res, next) {
  // console.log(`getMidiValuesV1()`)

  if (initializationError) {
    res.send(new errors.BadMethodError(`Could not initialize`))
    return next()

  }

  const list = [ ]
  for (const id in controls) {
    // console.log(`id=`, id)
    const ctrl = controls[id]
    list.push({
      id: ctrl.id,
      value: ctrl.value
    })
  }
  list.sort((a, b) => {
    if (a.id < b.id) {
      return -1
    }
    if (a.id > b.id) {
      return +1
    }
    return 0
  })
  // console.log(`list=`, list)

  res.send(list)
  return next()
}
