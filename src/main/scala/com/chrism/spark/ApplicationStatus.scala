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

sealed trait ApplicationStatus extends Product with Serializable

object ApplicationStatus {

  /** Indicates that the application ran successfully and it is ok to continue if the next stage exists */
  case object Success extends ApplicationStatus
  /** Indicates that the application failed and it is not ok to continue if the next stage exists */
  case object Failed extends ApplicationStatus
  /** Indicates that the application failed, but it is ok to continue if the next stage exists */
  case object FailedContinue extends ApplicationStatus
  /** Indicates that the application ran successfully, but the subsequent stages (if exist) should be skipped */
  case object Stop extends ApplicationStatus
  /** Indicates that the application was skipped and the subsequent stages (if exist) should also be skipped */
  case object SkippedStop extends ApplicationStatus
  /** Indicates that the application was skipped, but it is ok to run the subsequent stages (if exist) */
  case object SkippedContinue extends ApplicationStatus
  /** Indicates that the application was skipped as the previous stage failed
    * and the subsequent stages (if exist) should also be skipped
    */
  case object SkippedFailed extends ApplicationStatus
}
