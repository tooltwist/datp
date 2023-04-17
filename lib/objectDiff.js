/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */


// See https://stackoverflow.com/questions/8572826/generic-deep-diff-between-two-objects
function flattenObject(obj) {
  const object = Object.create(null);
  const path = [];
  const isObject = (value) => Object(value) === value;
 
  function dig(obj) {
   for (let [key, value] of Object.entries(obj)) {
     path.push(key);
     if (isObject(value)) dig(value);
     else object[path.join('.')] = value;
     path.pop();
   }
  }
 
  dig(obj);
  return object;
 }

 function diffFlatten(oldFlat, newFlat) {
  const updated = Object.assign({}, oldFlat);
  const removed = Object.assign({}, newFlat);

  /**delete the unUpdated keys*/
  for (let key in newFlat) {
      if (newFlat[key] === oldFlat[key]) {
           delete updated[key];
           delete removed[key];
      }
  }

  return { updated, removed }
}

function unflatenObject(flattenObject) {
  const unFlatten = Object.create(null);
  for (let [stringKeys, value] of Object.entries(flattenObject)) {
      let chain = stringKeys.split('.')
      let object = unFlatten

      for (let [i, key] of chain.slice(0, -1).entries()) {
          if (!object[key]) {
              let needArray = Number.isInteger(Number(chain[+i + 1]))
              object[key] = needArray ? [] : Object.create(null)
          }
          object = object[key];
      }
      let lastkey = chain.pop();
      object[lastkey] = value;
  }
  return unFlatten;
}

export function objectsAreTheSame(object1, object2) {
  const flat1 = flattenObject(object1)
  const flat2 = flattenObject(object2)
  const diff = diffFlatten(flat1, flat2)

  const numUpdates = Object.keys(diff.updated).length
  const numRemoved = Object.keys(diff.removed).length
  return (numUpdates === 0 && numRemoved === 0)
}

export function objectDiff(object1, object2) {

  // object2.yarp1 = 'yarp1'
  // object2.yarp2 = 'yarp2'
  // object2.transactionData.yarp3 = 'yarp3'
  // delete object2.status

  const flat1 = flattenObject(object1)
  // console.log(`flat1=`, flat1)
  const flat2 = flattenObject(object2)
  // console.log(`flat2=`, flat2)
  const diff = diffFlatten(flat1, flat2)
  // console.log(`diff=`, diff)

  // const u = unflatenObject(diff.updated)
  // console.log(`u=`, u)
  // const r = unflatenObject(diff.removed)
  // console.log(`r=`, r)

  return diff
}
