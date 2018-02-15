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

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CanaryScope implements Serializable {
  public CanaryScope(String scope, String region, Instant start, Instant end, String step, Map<String, String> extendedScopeParams) {
    this.scope = scope;
    this.region = region;
    this.start = start;
    this.end = end;
    this.step = step;
    this.extendedScopeParams = extendedScopeParams;
  }

  @JsonProperty public String scope;
  @JsonProperty public String region;
  @JsonProperty public Instant start;
  @JsonProperty public Instant end;
  @JsonProperty public String step;
  @JsonProperty public Map<String, String> extendedScopeParams;

  public CanaryScope copy() {
    return new CanaryScope(scope, region, start, end, step, new HashMap<>(extendedScopeParams));
  }
}
