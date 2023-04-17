/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { deepCopy } from '../../../lib/deepCopy'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {

})


test.serial('Clone nested values', async t => {
  const from = {
    a: 'abc',
    b: 123,
    c: {
      d: 456,
      e: 'def',
      f: {
        g: 789
      }
    }
  }
  const obj = deepCopy(from)
  t.is(obj.a, 'abc')
  t.is(obj.b, 123)
  t.truthy(typeof(obj.c) === 'object')
  t.is(obj.c.d, 456)
  t.is(obj.c.e, 'def')
  t.truthy(typeof(obj.c.f) === 'object')
  t.is(obj.c.f.g, 789)
})


test.serial('Merge nested values', async t => {
  const from = {
    a: 'abc',
    b: 123,
    c: {
      d: 456,
      e: 'def',
      f: {
        g: 789
      }
    }
  }
  const to = {
    c: {
      p: 987,
      q: 'ghi',
      r: {
        s: 'deep nested'
      }
    },
    x: {
      y: {
        z: 'this is z'
      }
    }
  }
  const obj = deepCopy(from, to)
  t.is(obj.a, 'abc')
  t.is(obj.b, 123)
  t.truthy(typeof(obj.c) === 'object')
  t.is(obj.c.d, 456)
  t.is(obj.c.e, 'def')
  t.truthy(typeof(obj.c.f) === 'object')
  t.is(obj.c.f.g, 789)
  t.is(obj.c.p, 987)
  t.is(obj.c.q, 'ghi')
  t.truthy(typeof(obj.c.r) === 'object')
  t.is(obj.c.r.s, 'deep nested')
  t.truthy(typeof(obj.x) === 'object')
  t.truthy(typeof(obj.x.y) === 'object')
  t.is(obj.x.y.z, 'this is z')
})
