/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.util.collection

/**
  * Sparce bit set is an open-addressing hash table with each entry holding 32 bit bitset.
  *
  */
class SparceBitSet(cap: Int) extends Serializable {
  import SparceBitSet._

  require((cap & (cap-1)) == 0, "capacity must be power of 2")
  private var table = Array.ofDim[Long](cap)
  private var _buckets = 0

  private[this] def _rehash(): Unit = {
    val newTable = Array.ofDim[Long](table.length*2)
    var i = 0
    while(i < table.length) {
      val x = table(i)

      if((x & _bucket_mask) != 0) {
        val h = shiftHash(x)
        val topIdx = x>>32
        val newSlot = SparceBitSet.locateSlot(h, topIdx, newTable)
        newTable(newSlot) = x // move 32bit at a time
      }
      i += 1
    }
    table = newTable
  }

  final def locateSlot(h: Int, topIdx: Long): Int = SparceBitSet.locateSlot(h, topIdx, table)

  final def add(value: Long): SparceBitSet = {
    if ((table.length - _buckets)  <= table.length/4) {
      _rehash()
    }
    val slot = locateSlot(hash(value), value>>5)
    val subSlot = 1L<<(value&31)
    val x = table(slot)
    table(slot) = x | subSlot | ((value>>5)<<32)
    if((x & _bucket_mask) == 0)
      _buckets += 1
    this
  }

  final def add(arr: Iterable[Long]): SparceBitSet = {
    for (a <- arr) {
      add(a)
    }
    this
  }

  final def remove(value: Long): SparceBitSet = {
    val h = hash(value)
    val slot = locateSlot(h, value>>5)
    val subSlot = 1L<<(value&31)
    val x = table(slot) & ~subSlot

    if ((x & _bucket_mask) != 0) {
      table(slot) = x
    }
    this
  }

  final def contains(value: Long): Boolean = {
    val slot = locateSlot(hash(value), value>>5)
    val subSlot = 1<<(value&31)
    (table(slot) & subSlot) != 0
  }

  // TODO: may use short-circuit
  final def supersetOf(that: SparceBitSet): Boolean = {
    var sum = true
    that.foreach(x => sum = sum && contains(x))
    sum
  }

  /**
    * Count number of values in an intersection of 2 sparce sets. This avoids allocating a new set
    * just to check the size of intersection.
    *
    * @param that - SparceBit set to intersect with
    * @return
    */
  final def intersectionSize(that: SparceBitSet): Int = {
    val (smaller, bigger) =
      if (this.table.length < that.table.length) (this, that)
      else (that, this)
    var cnt = 0
    var i = 0
    while(i < smaller.table.length) {
      val x = smaller.table(i)
      if((x & _bucket_mask) != 0) {
        val h = shiftHash(x)
        val topIdx = x>>32
        val y = bigger.table(bigger.locateSlot(h, topIdx))
        if ((y>>32) == topIdx && (y & _bucket_mask) != 0) {
          cnt += Integer.bitCount((x & y & _bucket_mask).toInt)
        }
      }
      i += 1
    }
    cnt
  }

  /**
    * Stopgap iteration primitive to avoid coding up a full-blown iterator.
    *
    * @param fn - function to apply to each integer key
    */
  final def foreach(fn: Long => Unit): Unit = {
    for (bucket <- table) {
      var v = bucket & _bucket_mask
      var i = 0
      val topIdx = (bucket>>32) * 32
      while (v != 0) {
        if ((v & 1) != 0) {
          fn(topIdx + i)
        }
        v >>= 1
        i += 1
      }
    }
  }

  final def eachBucket(fn: Long => Unit): Unit = {
    for (x <- table) {
      fn(x)
    }
  }
}

object SparceBitSet{
  private val _bucket_mask = (1L<<32) - 1

  def hash(value: Long): Int = {
    ((value >> 25) ^ (value >> 12) ^ (value >> 5)).toInt
  }

  // hash of value plus shift away the lower 32bits then shift back by 5
  def shiftHash(bucket: Long): Int = {
    ((bucket >> 52) ^ (bucket >> 39) ^ (bucket >> 32)).toInt
  }

  def locateSlot(h: Int, topIdx: Long, table: Array[Long]): Int = {
    var slot = h & (table.length-1)
    // search for empty bucket or existing bucket with the right index
    while({
      val x = table(slot)
      // not empty and top bits don't match
      (x != 0) && ((x>>32) != topIdx)
    })
    {
      slot += 1
      if(slot == table.length) slot = 0
    }
    slot
  }

  def apply(capacity: Int): SparceBitSet = new SparceBitSet(capacity)
}
