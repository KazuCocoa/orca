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

import com.fasterxml.jackson.annotation.JsonInclude

@JsonInclude(JsonInclude.Include.NON_EMPTY)
data class CanaryConfig(
  val metricsAccountName: String?,
  val storageAccountName: String?,
  val canaryConfigId: String?,
  val scopes: List<CanaryConfigScope> = emptyList(),
  val scoreThresholds: Thresholds = Thresholds(),
  val lifetimeHours: Int?,
  val beginCanaryAnalysisAfterMins: Int = 0,
  val lookbackMins: Int = 0,
  val canaryAnalysisIntervalMins: Int?
)
