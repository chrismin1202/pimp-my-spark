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

import com.chrism.commons.datatype.{CaseInsensitiveEnumLikeCompanionLike, EnumLike}

sealed abstract class FileFormat(override final val name: String) extends EnumLike

object FileFormat extends CaseInsensitiveEnumLikeCompanionLike[FileFormat] {

  override lazy val values: IndexedSeq[FileFormat] = IndexedSeq(Csv, Json, Orc, Parquet, Text)

  case object Csv extends FileFormat("csv")

  case object Json extends FileFormat("json")

  case object Orc extends FileFormat("orc")

  case object Parquet extends FileFormat("parquet")

  case object Text extends FileFormat("text")
}
