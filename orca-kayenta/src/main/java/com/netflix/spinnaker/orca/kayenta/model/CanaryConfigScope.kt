/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.orca.kayenta.model

import java.time.Instant

data class CanaryConfigScope(
  val scopeName: String?,
  val controlScope: String?,
  val controlRegion: String?,
  val experimentScope: String?,
  val experimentRegion: String?,
  val startTime: Instant?,
  val endTime: Instant?,
  val step: String = "60",
  val extendedScopeParams: Map<String, String> = emptyMap()
)
