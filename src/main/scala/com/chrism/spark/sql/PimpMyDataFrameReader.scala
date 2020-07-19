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

import com.chrism.spark.FileFormat
import org.apache.spark.sql.{DataFrame, DataFrameReader}

import scala.util.matching.Regex

trait PimpMyDataFrameReader {

  /** The object that contains SparkSQL-specific implicit methods.
    *
    * To use the implicit methods defined in this object, simply import the entire object in your scope:
    * {{{ import data_frame_implicits._ }}}
    */
  object data_frame_implicits {

    import PimpMyDataFrameReader.formatS3aPath

    implicit final class DataFrameReaderOps(reader: DataFrameReader) {

      /** Reads from S3 via s3a scheme.
        *
        * Before invoking this method, AWS access must be added to Hadoop configuration associated with
        * the SparkSession.
        *
        * The path can be specified in multiple ways:
        *   - The bucket name can be passed in as {{{ bucketName }}} and
        *     the relative sub-path within in the bucket can be passed in as its own string.
        *     ex) {{{ s3a(FileFormat.Text, "bucket-name", "path", "within", "bucket") }}}
        *   - The full path can be passed in as {{{ bucketName }}} parameter and
        *     {{{ relativePaths }}} can be omitted entirely.
        *     ex) {{{ s3a(FileFormat.Text, "bucket-name/path/within/bucket") }}}
        *   - The bucket name can be passed in as {{{ bucketName }}} and
        *     the entire relative path can be passed in as a single string.
        *     ex) {{{ s3a(FileFormat.Text, "bucket-name", "path/within/bucket") }}}
        *
        * @param format the [[FileFormat]] of the data in S3
        * @param bucketName the bucket name
        * @param relativePaths the relative path within the bucket
        * @return the [[DataFrame]] created from the data in the given S3 path
        */
      def s3a(format: FileFormat, bucketName: String, relativePaths: String*): DataFrame =
        reader.format(format.name).load(formatS3aPath(bucketName, relativePaths: _*))
    }
  }
}

private[sql] object PimpMyDataFrameReader {

  private[this] val PathSeparator: String = "/"
  /** Regex that only handles characters that are deemed safe to use in S3 */
  private[this] val S3PathRegex: Regex = new Regex(
    "(^" + PathSeparator + "+)?([a-zA-Z0-9!\\-_.*'()]+)(" + PathSeparator + "+$)?"
  )

  def formatS3aPath(bucketName: String, relativePaths: String*): String =
    "s3a://" + formatPath(bucketName +: relativePaths)

  private[this] def formatPath(pathComponents: Seq[String]): String =
    pathComponents.map(sanitizePath).mkString(PathSeparator)

  private[this] def sanitizePath(path: String): String =
    S3PathRegex.findAllMatchIn(path).map(_.group(2)).mkString(PathSeparator)
}
