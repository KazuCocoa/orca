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

package com.netflix.spinnaker.orca.kayenta.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Collections.emptyList;

public class CanaryConfig {
  @JsonProperty public String metricsAccountName;
  @JsonProperty public String storageAccountName;
  @JsonProperty public String canaryConfigId;
  @JsonProperty public List<CanaryConfigScope> scopes = emptyList();
  @JsonProperty public Thresholds scoreThresholds;
  @JsonProperty public Integer lifetimeHours;
  @JsonProperty public int beginCanaryAnalysisAfterMins = 0;
  @JsonProperty public int lookbackMins = 0;
  @JsonProperty public Integer canaryAnalysisIntervalMins;
}
