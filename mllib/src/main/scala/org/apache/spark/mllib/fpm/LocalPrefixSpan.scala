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

package org.apache.spark.mllib.fpm

import scala.collection.mutable

import org.apache.spark.Logging

/**
 * Calculate all patterns of a projected database in local.
 */
private[fpm] object LocalPrefixSpan extends Logging with Serializable {
  import PrefixSpan._
  /**
   * Calculate all patterns of a projected database.
   * @param minCount minimum count
   * @param maxPatternLength maximum pattern length
   * @param prefixes prefixes in reversed order
   * @param database the projected database
   * @return a set of sequential pattern pairs,
   *         the key of pair is sequential pattern (a list of items in reversed order),
   *         the value of pair is the pattern's count.
   */
  def run(
      minCount: Long,
      maxPatternLength: Int,
      prefixes: List[Set[Int]],
      database: Iterable[List[Set[Int]]]): Iterator[(List[Set[Int]], Long)] = {
    if (prefixes.length == maxPatternLength || database.isEmpty) {
      return Iterator.empty
    }
    val freqItemSetsAndCounts = getFreqItemAndCounts(minCount, database)
    val freqItems = freqItemSetsAndCounts.keys.flatten.toSet
    val filteredDatabase = database.map { suffix =>
      suffix
        .map(item => freqItems.intersect(item))
        .filter(_.nonEmpty)
    }
    freqItemSetsAndCounts.iterator.flatMap { case (item, count) =>
      val newPrefixes = item :: prefixes
      val newProjected = project(filteredDatabase, item)
      Iterator.single((newPrefixes, count)) ++
        run(minCount, maxPatternLength, newPrefixes, newProjected)
    }
  }

  /**
   * Calculate suffix sequence immediately after the first occurrence of an item.
   * @param item itemset to get suffix after
   * @param sequence sequence to extract suffix from
   * @return suffix sequence
   */
  def getSuffix(item: Set[Int], sequence: List[Set[Int]]): List[Set[Int]] = {
    val itemsetSeq = sequence
    val index = itemsetSeq.indexWhere(item.subsetOf(_))
    if (index == -1) {
      List()
    } else {
      itemsetSeq.drop(index + 1)
    }
  }

  def project(
      database: Iterable[List[Set[Int]]],
      prefix: Set[Int]): Iterable[List[Set[Int]]] = {
    database
      .map(getSuffix(prefix, _))
      .filter(_.nonEmpty)
  }

  /**
   * Generates frequent items by filtering the input data using minimal count level.
   * @param minCount the minimum count for an item to be frequent
   * @param database database of sequences
   * @return freq item to count map
   */
  private def getFreqItemAndCounts(
      minCount: Long,
      database: Iterable[List[Set[Int]]]): Map[Set[Int], Long] = {
    // TODO: use PrimitiveKeyOpenHashMap
    val counts = mutable.Map[Set[Int], Long]().withDefaultValue(0L)
    database.foreach { sequence =>
      sequence.flatMap(nonemptySubsets(_)).distinct.foreach { item =>
        counts(item) += 1L
      }
    }
    counts
      .filter { case (_, count) => count >= minCount }
      .toMap
  }
}
