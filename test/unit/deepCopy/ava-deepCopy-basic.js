/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { deepCopy } from '../../../lib/deepCopy'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {

})



test.serial('Simple object clone', async t => {

  const to = deepCopy({
    number: 123,
    word: 'abc'
  }, {
    word2: 'xyz'
  })
  t.is(to.number, 123)
  t.is(to.word, 'abc')
  t.is(to.word2, 'xyz')
})


test.serial('Missing second parameter', async t => {

  const to = deepCopy({
    a: 123,
    b: 'xyz'
  })
  t.is(to.a, 123)
  t.is(to.b, 'xyz')
})


test.serial('Overwrite simple values', async t => {
  const obj = {
    number: 999,
    word: 'xyz'
  }
  const newStuff = {
    word: 'abc',
    number: 123
  }
  deepCopy(newStuff, obj)
  t.is(obj.number, 123)
  t.is(obj.word, 'abc')
})
