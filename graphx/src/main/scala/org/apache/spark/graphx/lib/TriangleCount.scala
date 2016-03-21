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

package org.apache.spark.graphx.lib

import java.io._

import org.apache.spark.util.collection.{HashedBitSet, SparceBitSet}

import scala.reflect.ClassTag

import org.apache.spark.graphx._

/**
  * Compute the number of triangles passing through each vertex.
  *
  * The algorithm is relatively straightforward and can be computed in three steps:
  *
  * <ul>
  * <li>Compute the set of neighbors for each vertex
  * <li>For each edge compute the intersection of the sets and send the count to both vertices.
  * <li> Compute the sum at each vertex and divide by two since each triangle is counted twice.
  * </ul>
  *
  * Note that the input graph should have its edges in canonical direction
  * (i.e. the `sourceId` less than `destId`). Also the graph must have been partitioned
  * using [[org.apache.spark.graphx.Graph#partitionBy]].
  */
object TriangleCount {

  def check(set: HashedBitSet, set2: VertexSet): Unit = {
    for (v <- set2.iterator) if (!set.contains(v)) {
      println(set2.iterator.mkString("Seq(",",", ")"))
      assert(false)
    }
    for (v <- set) if (!set2.contains(v)) {
      println(set2.iterator.mkString("Seq(",",", ")"))
      assert(false)
    }
  }

  def run[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): Graph[Int, ED] = {
    // Remove redundant edges
    val g = graph.groupEdges((a, b) => a).cache()

    // Construct set representations of the neighborhoods
    val nbrSets: VertexRDD[(HashedBitSet, VertexSet)] =
      g.collectNeighborIds(EdgeDirection.Either).mapValues { (vid, nbrs) =>
        val set = new HashedBitSet(4)
        val set2 = new VertexSet(4)
        var i = 0
        while (i < nbrs.size) {
          // prevent self cycle
          if (nbrs(i) != vid) {
            set.add(nbrs(i))
            set2.add(nbrs(i))
          }
          i += 1
        }
        check(set, set2)
        (set, set2)
      }
    // join the sets with the graph
    val setGraph: Graph[(HashedBitSet, VertexSet), ED] = g.outerJoinVertices(nbrSets) {
      (vid, _, optSet) => optSet.getOrElse(null)
    }
    // Edge function computes intersection of smaller vertex with larger vertex
    def edgeFunc(ctx: EdgeContext[(HashedBitSet, VertexSet), ED, Int]) {
      assert(ctx.srcAttr != null)
      assert(ctx.dstAttr != null)

      for (v <- ctx.srcAttr._1) assert (ctx.srcAttr._2.contains(v))
      for (v <- ctx.srcAttr._2.iterator) assert (ctx.srcAttr._1.contains(v))

      for (v <- ctx.dstAttr._1) assert (ctx.dstAttr._2.contains(v))
      for (v <- ctx.dstAttr._2.iterator) assert (ctx.dstAttr._1.contains(v))

      var counter3 = 0
      for(v <- ctx.srcAttr._1) {
        if (v != ctx.srcId && v != ctx.dstId && ctx.dstAttr._1.contains(v)) {
          counter3 += 1
        }
      }
      val (smallSet, largeSet) = if (ctx.srcAttr._2.size < ctx.dstAttr._2.size) {
        (ctx.srcAttr._2, ctx.dstAttr._2)
      } else {
        (ctx.dstAttr._2, ctx.srcAttr._2)
      }
      val iter = smallSet.iterator
      var counter2: Int = 0
      while (iter.hasNext) {
        val vid = iter.next()
        if (vid != ctx.srcId && vid != ctx.dstId && largeSet.contains(vid)) {
          counter2 += 1
        }
      }
      ctx.srcAttr._1.remove(ctx.srcId).remove(ctx.dstId)

      val counter = ctx.srcAttr._1.intersectionSize(ctx.dstAttr._1)

      println(counter, counter2, counter3)
      if(counter != counter2) {
        val fileOut =
          new FileOutputStream("spark.ser")
        val out = new ObjectOutputStream(fileOut)
        out.writeObject(ctx.srcAttr._1)
        out.writeObject(ctx.dstAttr._1)
        out.writeObject(ctx.srcAttr._2)
        out.writeObject(ctx.dstAttr._2)
        out.close()
        fileOut.close()
        println(ctx.srcId, ctx.dstId)
        println(smallSet.iterator.mkString("Seq(",",", ")"))
        println(largeSet.iterator.mkString("Seq(",",", ")"))
        assert(false)
      }
      // assert(counter == counter2)
      ctx.sendToSrc(counter)
      ctx.sendToDst(counter)
    }
    // compute the intersection along edges1/2acdltx
    val counters: VertexRDD[Int] = setGraph.aggregateMessages(edgeFunc, _ + _)
    // Merge counters with the graph and divide by two since each triangle is counted twice
    g.outerJoinVertices(counters) {
      (vid, _, optCounter: Option[Int]) =>
        val dblCount = optCounter.getOrElse(0)
        // double count should be even (divisible by two)
        // assert((dblCount & 1) == 0)
        dblCount / 2
    }
  } // end of TriangleCount
}
