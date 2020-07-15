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
package com.chrism.spark.sql

import com.chrism.commons.FunTestSuite
import com.chrism.spark.SparkTestSuiteLike

final class PimpMySparkSqlTest extends FunTestSuite with SparkTestSuiteLike with PimpMySparkSql {

  import PimpMySparkSqlTest._

  test("INNER JOIN: joinWithUsingThenMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, ErlichBachman, BertramGilfoyle, DineshChugtai, JaredDunn))
    val employmentDs = spark.createDataset(
      Seq(
        RichardEmployment,
        ErlichEmployment,
        GilfoyleEmployment,
        DineshEmployment,
        JaredEmployment,
      ))
    val profileDs = personDs.joinWithUsingThenMap(employmentDs, "name")(Profile.apply)

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(ErlichBachman, ErlichEmployment),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
      Profile(JaredDunn, JaredEmployment),
    )
  }

  test("LEFT OUTER JOIN: leftOuterJoinWithUsingThenMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, ErlichBachman, BertramGilfoyle, DineshChugtai, JaredDunn))
    val employmentDs = spark.createDataset(Seq(RichardEmployment, GilfoyleEmployment, DineshEmployment))
    val profileDs = personDs.leftOuterJoinWithUsingThenMap(employmentDs, "name")((l, r) => Profile(l, r.orNull))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(ErlichBachman, null),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
      Profile(JaredDunn, null),
    )
  }

  test("LEFT OUTER JOIN: leftOuterJoinWithUsingThenFlatMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, ErlichBachman, BertramGilfoyle, DineshChugtai, JaredDunn))
    val employmentDs = spark.createDataset(Seq(RichardEmployment, GilfoyleEmployment, DineshEmployment))
    val profileDs = personDs.leftOuterJoinWithUsingThenFlatMap(employmentDs, "name")((l, r) => r.map(Profile(l, _)))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
    )
  }

  test("RIGHT OUTER JOIN: rightOuterJoinWithUsingThenMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, BertramGilfoyle, DineshChugtai))
    val employmentDs = spark.createDataset(
      Seq(
        RichardEmployment,
        ErlichEmployment,
        GilfoyleEmployment,
        DineshEmployment,
        JaredEmployment,
      ))
    val profileDs = personDs.rightOuterJoinWithUsingThenMap(employmentDs, "name")((l, r) => Profile(l.orNull, r))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(null, ErlichEmployment),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
      Profile(null, JaredEmployment),
    )
  }

  test("RIGHT OUTER JOIN: rightOuterJoinWithUsingThenFlatMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, BertramGilfoyle, DineshChugtai))
    val employmentDs = spark.createDataset(
      Seq(
        RichardEmployment,
        ErlichEmployment,
        GilfoyleEmployment,
        DineshEmployment,
        JaredEmployment,
      ))
    val profileDs = personDs.rightOuterJoinWithUsingThenFlatMap(employmentDs, "name")((l, r) => l.map(Profile(_, r)))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
    )
  }

  test("FULL OUTER JOIN: fullOuterJoinWithUsingThenMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, ErlichBachman, BertramGilfoyle, DineshChugtai))
    val employmentDs = spark.createDataset(
      Seq(
        RichardEmployment,
        GilfoyleEmployment,
        DineshEmployment,
        JaredEmployment,
      ))
    val profileDs = personDs.fullOuterJoinWithUsingThenMap(employmentDs, "name")((l, r) => Profile(l.orNull, r.orNull))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(ErlichBachman, null),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
      Profile(null, JaredEmployment),
    )
  }

  test("FULL OUTER JOIN: fullOuterJoinWithUsingThenFlatMap") {
    import spark.implicits._
    import spark_sql_implicits._

    val personDs = spark.createDataset(Seq(RichardHendricks, ErlichBachman, BertramGilfoyle, DineshChugtai))
    val employmentDs = spark.createDataset(
      Seq(
        RichardEmployment,
        GilfoyleEmployment,
        DineshEmployment,
        JaredEmployment,
      ))
    val profileDs = personDs.fullOuterJoinWithUsingThenFlatMap(employmentDs, "name")((l, r) =>
      for {
        p <- l
        e <- r
      } yield Profile(p, e))

    profileDs.collect() should contain theSameElementsAs Seq(
      Profile(RichardHendricks, RichardEmployment),
      Profile(BertramGilfoyle, GilfoyleEmployment),
      Profile(DineshChugtai, DineshEmployment),
    )
  }
}

private[this] object PimpMySparkSqlTest {

  private val RichardHendricks: Person = Person("Richard Hendricks", Seq("compression", "tab"))
  private val RichardEmployment: Employment = Employment("Richard Hendricks", "Pied Piper")
  private val ErlichBachman: Person = Person("Erlich Bachman", Seq("entrepreneur", "cannabis"))
  private val ErlichEmployment: Employment = Employment("Erlich Bachman", "Pied Piper")
  private val BertramGilfoyle: Person = Person("Bertram Gilfoyle", Seq("system architecture"))
  private val GilfoyleEmployment: Employment = Employment("Bertram Gilfoyle", "Pied Piper")
  private val DineshChugtai: Person = Person("Dinesh Chugtai", Seq("Java", "Scala"))
  private val DineshEmployment: Employment = Employment("Dinesh Chugtai", "Pied Piper")
  private val JaredDunn: Person = Person("Jared Dunn", Seq("business development"))
  private val JaredEmployment: Employment = Employment("Jared Dunn", "Pied Piper")

  private final case class Person(name: String, skills: Seq[String])

  private final case class Employment(name: String, employer: String)

  private final case class Profile(name: String, employer: String, skills: Seq[String])

  private object Profile {

    def apply(p: Person, e: Employment): Profile =
      Profile(
        if (p == null) null else p.name,
        if (e == null) null else e.employer,
        if (p == null) Seq.empty else p.skills)
  }
}
