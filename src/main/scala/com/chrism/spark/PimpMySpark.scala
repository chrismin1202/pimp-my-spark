/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *      http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.chrism.spark

import org.apache.spark.sql.{Column, Dataset, Encoder}

/** A trait for supplementing Spark related methods via "pimp-my-library" pattern */
trait PimpMySpark {

  /** The object that contains SparkSQL-specific implicit methods.
    *
    * To use the implicit methods defined in this object, simply import the entire object in your scope:
    * {{{ import spark_sql_implicits._ }}}
    */
  object spark_sql_implicits {

    /** Implicitly applies SQL operations to the given [[Dataset]] instance.
      *
      * @param left the left [[Dataset]] to apply SQL operations
      * @tparam L the type of the objects in the given [[Dataset]] instance
      */
    implicit final class DatasetSqlOps[L](left: Dataset[L]) {

      // TODO: Consider supporting CROSS JOIN

      /** INNER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def joinWithUsing[R](right: Dataset[R], column: String, moreColumns: String*): Dataset[(L, R)] =
        left.joinWith(right, toJoinCondition(right, column, moreColumns: _*))

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} using the given join condition
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that this method is not null-safe. When LEFT OUTER JOINing,
        * the right value in the resulting [[Dataset]] can be null
        * if no matching record is found in the right [[Dataset]].
        * Consider using [[leftOuterJoinWithThenMap()]] or [[leftOuterJoinWithThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def leftOuterJoinWith[R](right: Dataset[R], condition: Column): Dataset[(L, R)] =
        left.joinWith(right, condition, "left_outer")

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * Also note that this method is not null-safe. When LEFT OUTER JOINing,
        * the right value in the resulting [[Dataset]] can be null
        * if no matching record is found in the right [[Dataset]].
        * Consider using [[leftOuterJoinWithUsingThenMap()]] or [[leftOuterJoinWithUsingThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def leftOuterJoinWithUsing[R](right: Dataset[R], column: String, moreColumns: String*): Dataset[(L, R)] =
        leftOuterJoinWith(right, toJoinCondition(right, column, moreColumns: _*))

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} using the given join condition
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that this method is not null-safe. When RIGHT OUTER JOINing,
        * the left value in the resulting [[Dataset]] can be null
        * if no matching record is found in the left [[Dataset]].
        * Consider using [[rightOuterJoinWithThenMap()]] or [[rightOuterJoinWithThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def rightOuterJoinWith[R](right: Dataset[R], condition: Column): Dataset[(L, R)] =
        left.joinWith(right, condition, "right_outer")

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * Also note that this method is not null-safe. When RIGHT OUTER JOINing,
        * the left value in the resulting [[Dataset]] can be null
        * if no matching record is found in the left [[Dataset]].
        * Consider using [[rightOuterJoinWithUsingThenMap()]] or [[rightOuterJoinWithUsingThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def rightOuterJoinWithUsing[R](right: Dataset[R], column: String, moreColumns: String*): Dataset[(L, R)] =
        rightOuterJoinWith(right, toJoinCondition(right, column, moreColumns: _*))

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} using the given join condition
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that this method is not null-safe. When FULL OUTER JOINing,
        * the left value or the right value in the resulting [[Dataset]] can be null
        * if no matching record is found in either [[Dataset]].
        * Consider using [[fullOuterJoinWithThenMap()]] or [[fullOuterJoinWithThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def fullOuterJoinWith[R](right: Dataset[R], condition: Column): Dataset[(L, R)] =
        left.joinWith(right, condition, "full_outer")

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and returns [[Dataset]] of tuple of [[L]] and [[R]].
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * Also note that this method is not null-safe. When FULL OUTER JOINing,
        * the left value or the right value in the resulting [[Dataset]] can be null
        * if no matching record is found in either [[Dataset]].
        * Consider using [[fullOuterJoinWithUsingThenMap()]] or [[fullOuterJoinWithUsingThenFlatMap()]].
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @return the joined [[Dataset]] of tuple of [[L]] and [[R]]
        */
      def fullOuterJoinWithUsing[R](right: Dataset[R], column: String, moreColumns: String*): Dataset[(L, R)] =
        fullOuterJoinWith(right, toJoinCondition(right, column, moreColumns: _*))

      /** INNER JOINs {{{ left }}} with {{{ right }}} using the given join condition
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def joinWithThenMap[R, J: Encoder](right: Dataset[R], condition: Column)(f: (L, R) => J): Dataset[J] =
        left.joinWith(right, condition).map(f.tupled)

      /** INNER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def joinWithUsingThenMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (L, R) => J
      ): Dataset[J] =
        joinWithThenMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** INNER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def joinWithThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (L, R) => TraversableOnce[J]
      ): Dataset[J] =
        left.joinWith(right, condition).flatMap(f.tupled)

      /** INNER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def joinWithUsingThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (L, R) => TraversableOnce[J]
      ): Dataset[J] =
        joinWithThenFlatMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def leftOuterJoinWithThenMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (L, Option[R]) => J
      ): Dataset[J] =
        leftOuterJoinWith(right, condition).map(r => f(r._1, Option(r._2)))

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def leftOuterJoinWithUsingThenMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (L, Option[R]) => J
      ): Dataset[J] =
        leftOuterJoinWithThenMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def leftOuterJoinWithThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (L, Option[R]) => TraversableOnce[J]
      ): Dataset[J] =
        leftOuterJoinWith(right, condition).flatMap(r => f(r._1, Option(r._2)))

      /** LEFT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def leftOuterJoinWithUsingThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (L, Option[R]) => TraversableOnce[J]
      ): Dataset[J] =
        leftOuterJoinWithThenFlatMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def rightOuterJoinWithThenMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (Option[L], R) => J
      ): Dataset[J] =
        rightOuterJoinWith(right, condition).map(r => f(Option(r._1), r._2))

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def rightOuterJoinWithUsingThenMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (Option[L], R) => J
      ): Dataset[J] =
        rightOuterJoinWithThenMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def rightOuterJoinWithThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (Option[L], R) => TraversableOnce[J]
      ): Dataset[J] =
        rightOuterJoinWith(right, condition).flatMap(r => f(Option(r._1), r._2))

      /** RIGHT OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def rightOuterJoinWithUsingThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (Option[L], R) => TraversableOnce[J]
      ): Dataset[J] =
        rightOuterJoinWithThenFlatMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def fullOuterJoinWithThenMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (Option[L], Option[R]) => J
      ): Dataset[J] =
        fullOuterJoinWith(right, condition).map(r => f(Option(r._1), Option(r._2)))

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given mapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def fullOuterJoinWithUsingThenMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (Option[L], Option[R]) => J
      ): Dataset[J] =
        fullOuterJoinWithThenMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * @param right the right [[Dataset]] to join with
        * @param condition the join condition expressed using [[Column]]
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def fullOuterJoinWithThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        condition: Column
      )(
        f: (Option[L], Option[R]) => TraversableOnce[J]
      ): Dataset[J] =
        fullOuterJoinWith(right, condition).flatMap(r => f(Option(r._1), Option(r._2)))

      /** FULL OUTER JOINs {{{ left }}} with {{{ right }}} on the specified column(s)
        * and then apply the given flatMapping function {{{ f }}} to every row.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @param f the mapping function
        * @tparam R the type of the objects in the right [[Dataset]] instance
        * @tparam J the type of the objects in the joined [[Dataset]] instance
        * @return the joined [[Dataset]] of type [[J]]
        */
      def fullOuterJoinWithUsingThenFlatMap[R, J: Encoder](
        right: Dataset[R],
        column: String,
        moreColumns: String*
      )(
        f: (Option[L], Option[R]) => TraversableOnce[J]
      ): Dataset[J] =
        fullOuterJoinWithThenFlatMap(right, toJoinCondition(right, column, moreColumns: _*))(f)

      /** Constructs join condition with the given column names.
        *
        * Note that all column names must exist in both [[Dataset]] instances.
        *
        * @param right the right [[Dataset]] to join with
        * @param column the name of the column to use for joining
        * @param moreColumns additional column names to join on
        * @return the join expression as an instance of [[Column]]
        */
      private[this] def toJoinCondition(right: Dataset[_], column: String, moreColumns: String*): Column =
        (column +: moreColumns).map(c => left(c) === right(c)).reduce(_ && _)
    }
  }
}
