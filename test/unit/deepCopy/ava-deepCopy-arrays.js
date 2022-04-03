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



test.serial('Copy array of numbers', async t => {
  const to = deepCopy({
    a: [ 1, 2, 5.6 ]
  })
  t.truthy(Array.isArray(to.a))
  t.is(to.a.length, 3)
  t.is(to.a[0], 1)
  t.is(to.a[1], 2)
  t.is(to.a[2], 5.6)
})


test.serial('Copy array of strings', async t => {
  const to = deepCopy({
    words: [ 'this', 'is', 'very', 'nice' ]
  })
  // console.log(`to=`, to)
  t.truthy(Array.isArray(to.words))
  t.is(to.words.length, 4)
  t.is(to.words[0], 'this')
  t.is(to.words[1], 'is')
  t.is(to.words[2], 'very')
  t.is(to.words[3], 'nice')
})


test.serial('Copy array of objects', async t => {
  const to = deepCopy({
    words: [
      { word: 'this' },
      { word: 'is', len: 2 },
      { str: 'cool', rating: 9.5 }
    ]
  })
  // console.log(`to=`, to)
  t.truthy(Array.isArray(to.words))
  t.is(to.words.length, 3)
  t.is(typeof(to.words[0]), 'object')
  t.is(to.words[0].word, 'this')
  t.is(typeof(to.words[1]), 'object')
  t.is(to.words[1].word, 'is')
  t.is(to.words[1].len, 2)
  t.is(typeof(to.words[2]), 'object')
  t.is(to.words[2].str, 'cool')
  t.is(to.words[2].rating, 9.5)
})


test.serial('Overwrite array', async t => {
  const original = {
    words: [ 'this', 'is', 'very', 'nice' ]
  }
  const newStuff = {
    words: [ 'these', 'are', 'much', 'better', 'words' ]
  }
  const to = deepCopy(newStuff, original)
  // console.log(`to=`, to)
  t.truthy(Array.isArray(to.words))
  t.is(to.words.length, 5)
  t.is(to.words[0], 'these')
  t.is(to.words[1], 'are')
  t.is(to.words[2], 'much')
  t.is(to.words[3], 'better')
  t.is(to.words[4], 'words')
})


test.serial('Clone array of arrays', async t => {
  const to = deepCopy({
    matrix: [
      [1, 2],
      ['a', 'b', 'c'],
      [ { some: 'thing' }, { other: 'stuff' }]
    ]
  })

  // console.log(`to=`, to)
  t.truthy(Array.isArray(to.matrix))
  t.is(to.matrix.length, 3)

  t.truthy(Array.isArray(to.matrix[0]))
  t.is(to.matrix[0].length, 2)
  t.is(to.matrix[0][0], 1)
  t.is(to.matrix[0][1], 2)

  t.truthy(Array.isArray(to.matrix[1]))
  t.is(to.matrix[1].length, 3)
  t.is(to.matrix[1][0], 'a')
  t.is(to.matrix[1][1], 'b')
  t.is(to.matrix[1][2], 'c')

  t.truthy(Array.isArray(to.matrix[2]))
  t.is(to.matrix[2].length, 2)
  t.is(typeof(to.matrix[2][0]), 'object')
  t.is(to.matrix[2][0].some, 'thing')
  t.is(typeof(to.matrix[2][1]), 'object')
  t.is(to.matrix[2][1].other, 'stuff')
})
