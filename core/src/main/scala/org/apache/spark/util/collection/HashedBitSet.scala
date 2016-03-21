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

import java.lang.Long.bitCount

class HashedBitSet(initialCapacity: Int) extends Serializable {
  import HashedBitSet._

  val table = new OpenHashMap[Long, Long](initialCapacity)

  def add(value: Long): HashedBitSet = {
    val key = value >> _shift
    val subKey = value & _mask
    val x = table(key)
    table(key) = x | (1L<<subKey)
    this
  }

  def contains(value: Long): Boolean = {
    val key = value >> _shift
    val x = table(key)
    val subKey = value & _mask
    (x & (1L<<subKey)) != 0
  }

  def remove(value: Long): HashedBitSet = {
    val key = value >> _shift
    val x = table(key)
    val subKey = value & _mask
    table(key) = x & ~(1L<<subKey)
    this
  }

  def intersectionSize(that: HashedBitSet): Int = {
    val (small, large) = if (table.size < that.table.size) (this, that) else (that, this)
    val iter = small.table.iterator
    var count = 0
    while(iter.hasNext) {
      val x = iter.next()
      val y = that.table(x._1)
      if (y != 0) {
        count += bitCount(x._2 & y)
      }
    }
    count
  }

  def foreach(fn: Long => Unit): Unit = {
    for (bucket <- table) {
      var v = bucket._2
      val topIdx = bucket._1<<_shift
      var i = 0
      while (v != 0) {
        if ((v & 1) != 0) {
          fn(topIdx + i)
        }
        v >>= 1
        i += 1
      }
    }
  }
}

object HashedBitSet{
  val _mask = 31
  val _shift = 5
}
