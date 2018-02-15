/*
 * Copyright 2017 Google, Inc.
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

package com.netflix.spinnaker.orca.kayenta.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.spinnaker.orca.jackson.OrcaObjectMapper;
import com.netflix.spinnaker.orca.kayenta.model.CanaryConfig;
import com.netflix.spinnaker.orca.kayenta.model.CanaryConfigScope;
import com.netflix.spinnaker.orca.kayenta.model.CanaryScope;
import com.netflix.spinnaker.orca.kayenta.model.RunCanaryContext;
import com.netflix.spinnaker.orca.kayenta.tasks.AggregateCanaryResultsTask;
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder;
import com.netflix.spinnaker.orca.pipeline.TaskNode.Builder;
import com.netflix.spinnaker.orca.pipeline.WaitStage;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS;
import static com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder.newStage;
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Collections.singletonMap;

@Component
public class KayentaCanaryStage implements StageDefinitionBuilder {

  private final Clock clock;
  private final WaitStage waitStage;
  private final ObjectMapper mapper;

  @Autowired
  public KayentaCanaryStage(Clock clock, WaitStage waitStage) {
    this.clock = clock;
    this.waitStage = waitStage;
    this.mapper = OrcaObjectMapper.newInstance()
      .disable(WRITE_DATES_AS_TIMESTAMPS); // we want Instant serialized as ISO string
  }

  @Override
  public void taskGraph(Stage stage, Builder builder) {
    builder.withTask("aggregateCanaryResults", AggregateCanaryResultsTask.class);
  }

  @Override
  public List<Stage> aroundStages(Stage stage) {
    CanaryConfig canaryConfig = stage.mapTo("/canaryConfig", CanaryConfig.class);
    List<CanaryConfigScope> configScopes = canaryConfig.scopes;

    if (configScopes.isEmpty()) {
      throw new IllegalArgumentException("Canary stage configuration must contain at least one scope.");
    }

    // Using time boundaries from just the first scope since it doesn't really make sense for each scope to have different boundaries.
    // TODO(duftler): Add validation to log warning when time boundaries differ across scopes.
    CanaryConfigScope firstScope = configScopes.get(0);

    int lifetimeMinutes;
    if (firstScope.endTime != null) {
      lifetimeMinutes = (int) Optional.ofNullable(firstScope.startTime).orElseGet(() -> Instant.now(clock)).until(firstScope.endTime, MINUTES);
    } else if (canaryConfig.lifetimeHours != null) {
      lifetimeMinutes = (int) Duration.ofHours(canaryConfig.lifetimeHours).toMinutes();
    } else {
      throw new IllegalArgumentException("Canary stage configuration must include either `endTime` or `lifetimeHours`.");
    }

    int canaryAnalysisIntervalMins = Optional.ofNullable(canaryConfig.canaryAnalysisIntervalMins).orElse(lifetimeMinutes);

    if (canaryAnalysisIntervalMins == 0 || canaryAnalysisIntervalMins > lifetimeMinutes) {
      canaryAnalysisIntervalMins = lifetimeMinutes;
    }

    int numIntervals = lifetimeMinutes / canaryAnalysisIntervalMins;
    List<Stage> stages = new ArrayList<>();

    if (canaryConfig.beginCanaryAnalysisAfterMins > 0L) {
      stages.add(newStage(
        stage.getExecution(),
        waitStage.getType(),
        "Warmup Wait",
        singletonMap("waitTime", Duration.ofMinutes(canaryConfig.beginCanaryAnalysisAfterMins).getSeconds()),
        stage,
        STAGE_BEFORE
      ));
    }

    for (int i = 1; i <= numIntervals; i++) {
      // If an end time was explicitly specified, we don't need to synchronize the execution of the canary pipeline with the real time.
      if (firstScope.endTime == null) {
        stages.add(newStage(
          stage.getExecution(),
          waitStage.getType(),
          "Interval Wait #" + i,
          singletonMap("waitTime", Duration.ofMinutes(canaryAnalysisIntervalMins).getSeconds()),
          stage,
          STAGE_BEFORE
        ));
      }

      RunCanaryContext runCanaryContext = new RunCanaryContext(
        canaryConfig.metricsAccountName,
        canaryConfig.storageAccountName,
        canaryConfig.canaryConfigId,
        buildRequestScopes(configScopes),
        canaryConfig.scoreThresholds
      );

      for (Map.Entry<String, Map<String, CanaryScope>> entry : runCanaryContext.scopes.entrySet()) {
        Instant start, end;

        if (firstScope.endTime == null) {
          start = Optional.ofNullable(firstScope.startTime).orElseGet(() -> Instant.now(clock)).plus(canaryConfig.beginCanaryAnalysisAfterMins, MINUTES);
          end = Optional.ofNullable(firstScope.startTime).orElseGet(() -> Instant.now(clock)).plus(canaryConfig.beginCanaryAnalysisAfterMins + i * canaryAnalysisIntervalMins, MINUTES);
        } else {
          start = Optional.ofNullable(firstScope.startTime).orElseGet(() -> Instant.now(clock));
          end = Optional.ofNullable(firstScope.startTime).orElseGet(() -> Instant.now(clock)).plus(i * canaryAnalysisIntervalMins, MINUTES);
        }

        if (canaryConfig.lookbackMins > 0) {
          start = end.minus(canaryConfig.lookbackMins, MINUTES);
        }

        Map<String, CanaryScope> contextScope = entry.getValue();
        CanaryScope controlScope = contextScope.get("controlScope");
        controlScope.start = start;
        controlScope.end = end;
        CanaryScope experimentScope = contextScope.get("experimentScope");
        experimentScope.start = start;
        experimentScope.end = end;
      }

      stages.add(newStage(
        stage.getExecution(),
        RunCanaryPipelineStage.STAGE_TYPE,
        "Run Canary #" + i,
        mapper.convertValue(runCanaryContext, Map.class),
        stage,
        STAGE_BEFORE
      ));
    }

    return stages;
  }

  @Nonnull
  public Map<String, Map<String, CanaryScope>> buildRequestScopes(List<CanaryConfigScope> configScopes) {
    Map<String, Map<String, CanaryScope>> requestScopes = new HashMap<>();
    configScopes.forEach(configScope -> {
      CanaryScope controlScope = new CanaryScope(
        configScope.controlScope,
        configScope.controlRegion,
        configScope.startTime,
        configScope.endTime,
        configScope.step,
        configScope.extendedScopeParams
      );
      CanaryScope experimentScope = new CanaryScope(
        configScope.experimentScope,
        configScope.experimentRegion,
        configScope.startTime,
        configScope.endTime,
        configScope.step,
        configScope.extendedScopeParams
      );

      requestScopes.put(configScope.scopeName, mapOf(
        entry("controlScope", controlScope),
        entry("experimentScope", experimentScope)
      ));
    });
    return requestScopes;
  }

  @Override
  public String getType() {
    return "kayentaCanary";
  }

  private static <K, V> Pair<K, V> entry(K key, V value) {
    return new ImmutablePair<>(key, value);
  }

  private static <K, V> Map<K, V> mapOf(Pair<K, V>... entries) {
    Map<K, V> map = new HashMap<>();
    for (Pair<K, V> entry : entries) {
      map.put(entry.getLeft(), entry.getRight());
    }
    return map;
  }
}
