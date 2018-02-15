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

import com.google.common.base.Strings;
import com.netflix.spinnaker.orca.kayenta.tasks.AggregateCanaryResultsTask;
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder;
import com.netflix.spinnaker.orca.pipeline.TaskNode;
import com.netflix.spinnaker.orca.pipeline.WaitStage;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.*;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.function.Consumer;

import static com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder.newStage;
import static com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE;
import static java.lang.Long.parseLong;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

@Component
public class KayentaCanaryStage implements StageDefinitionBuilder {

  private final Clock clock;
  private final WaitStage waitStage;

  @Autowired
  public KayentaCanaryStage(Clock clock, WaitStage waitStage) {
    this.clock = clock;
    this.waitStage = waitStage;
  }

  @Override
  public void taskGraph(Stage stage, TaskNode.Builder builder) {
    builder.withTask("aggregateCanaryResults", AggregateCanaryResultsTask.class);
  }

  @Override
  public List<Stage> aroundStages(Stage stage) {
    Map<String, Object> context = stage.getContext();
    Map<String, Object> canaryConfig = (Map<String, Object>) context.get("canaryConfig");
    String metricsAccountName = (String) canaryConfig.get("metricsAccountName");
    String storageAccountName = (String) canaryConfig.get("storageAccountName");
    String canaryConfigId = (String) canaryConfig.get("canaryConfigId");
    List<Map<String, Object>> configScopes = (List<Map<String, Object>>) canaryConfig.get("scopes");

    if (configScopes.isEmpty()) {
      throw new IllegalArgumentException("Canary stage configuration must contain at least one scope.");
    }

    Map<String, Map> requestScopes = new HashMap<>();

    configScopes.forEach(configScope -> {
      // TODO(duftler): Externalize these default values.
      String scopeName = (String) configScope.getOrDefault("scopeName", "default");
      String controlScope = (String) configScope.get("controlScope");
      String controlRegion = (String) configScope.get("controlRegion");
      String experimentScope = (String) configScope.get("experimentScope");
      String experimentRegion = (String) configScope.get("experimentRegion");
      String startTimeIso = (String) configScope.get("startTimeIso");
      String endTimeIso = (String) configScope.get("endTimeIso");
      String step = (String) configScope.getOrDefault("step", "60");
      Map<String, String> extendedScopeParams = (Map<String, String>) configScope.getOrDefault("extendedScopeParams", emptyMap());
      Map requestScope = map(rs -> {
        Consumer<Map<Object, Object>> scope = cs -> {
          cs.put("scope", controlScope);
          cs.put("region", controlRegion);
          cs.put("start", startTimeIso);
          cs.put("end", endTimeIso);
          cs.put("step", step);
          cs.put("extendedScopeParams", extendedScopeParams);
        };
        rs.put("controlScope", map(scope));
        rs.put("experimentScope", map(scope));
      });

      requestScopes.put(scopeName, requestScope);
    });

    // Using time boundaries from just the first scope since it doesn't really make sense for each scope to have different boundaries.
    // TODO(duftler): Add validation to log warning when time boundaries differ across scopes.
    Map<String, Object> firstScope = configScopes.get(0);
    String startTimeIso = (String) firstScope.getOrDefault("startTimeIso", Instant.now(clock).toString());
    Instant startTimeInstant = Instant.parse(startTimeIso);
    String endTimeIso = (String) firstScope.get("endTimeIso");
    Instant endTimeInstant;
    Map<String, String> scoreThresholds = (Map<String, String>) canaryConfig.get("scoreThresholds");
    String lifetimeHours = (String) canaryConfig.get("lifetimeHours");
    long lifetimeMinutes;
    long beginCanaryAnalysisAfterMins = longFromMap(canaryConfig, "beginCanaryAnalysisAfterMins");
    long lookbackMins = longFromMap(canaryConfig, "lookbackMins");

    if (endTimeIso != null) {
      endTimeInstant = Instant.parse(endTimeIso);
      lifetimeMinutes = startTimeInstant.until(endTimeInstant, ChronoUnit.MINUTES);
    } else if (lifetimeHours != null) {
      lifetimeMinutes = Duration.ofHours(parseLong(lifetimeHours)).toMinutes();
    } else {
      throw new IllegalArgumentException("Canary stage configuration must include either `endTimeIso` or `lifetimeHours`.");
    }

    long canaryAnalysisIntervalMins = longFromMap(canaryConfig, "canaryAnalysisIntervalMins", lifetimeMinutes);

    if (canaryAnalysisIntervalMins == 0 || canaryAnalysisIntervalMins > lifetimeMinutes) {
      canaryAnalysisIntervalMins = lifetimeMinutes;
    }

    long numIntervals = lifetimeMinutes / canaryAnalysisIntervalMins;
    List<Stage> stages = new ArrayList<>();

    if (beginCanaryAnalysisAfterMins > 0L) {
      Map<String, Object> warmupWaitContext = mapOf(entry("waitTime", Duration.ofMinutes(beginCanaryAnalysisAfterMins).getSeconds()));

      stages.add(newStage(stage.getExecution(), waitStage.getType(), "Warmup Wait", warmupWaitContext, stage, STAGE_BEFORE));
    }

    for (int i = 1; i <= numIntervals; i++) {
      // If an end time was explicitly specified, we don't need to synchronize the execution of the canary pipeline with the real time.
      if (endTimeIso == null) {
        Map<String, Object> intervalWaitContext = singletonMap(
          "waitTime", Duration.ofMinutes(canaryAnalysisIntervalMins).getSeconds()
        );

        stages.add(newStage(stage.getExecution(), waitStage.getType(), "Interval Wait #" + i, intervalWaitContext, stage, STAGE_BEFORE));
      }

      Map<String, Object> runCanaryContext = mapOf(
        entry("metricsAccountName", metricsAccountName),
        entry("storageAccountName", storageAccountName),
        entry("canaryConfigId", canaryConfigId),
        entry("scopes", deepCopy(requestScopes)),
        entry("scoreThresholds", scoreThresholds)
      );

      for (Map.Entry<String, Map<String, Object>> entry : ((Map<String, Map<String, Object>>) runCanaryContext.get("scopes")).entrySet()) {
        Map<String, Object> contextScope = entry.getValue();
        Map<String, Object> controlScope = (Map<String, Object>) contextScope.get("controlScope");
        if (endTimeIso == null) {
          controlScope.put("start", startTimeInstant.plus(beginCanaryAnalysisAfterMins, ChronoUnit.MINUTES).toString());
          controlScope.put("end", startTimeInstant.plus(beginCanaryAnalysisAfterMins + i * canaryAnalysisIntervalMins, ChronoUnit.MINUTES).toString());
        } else {
          controlScope.put("start", startTimeInstant.toString());
          controlScope.put("end", startTimeInstant.plus(i * canaryAnalysisIntervalMins, ChronoUnit.MINUTES).toString());
        }

        if (lookbackMins > 0) {
          controlScope.put("start", Instant.parse(controlScope.get("end").toString()).minus(lookbackMins, ChronoUnit.MINUTES).toString());
        }

        Map<String, Object> experimentScope = (Map<String, Object>) contextScope.get("experimentScope");
        experimentScope.put("start", controlScope.get("start"));
        experimentScope.put("end", controlScope.get("end"));
      }

      stages.add(newStage(stage.getExecution(), RunCanaryPipelineStage.STAGE_TYPE, "Run Canary #" + i, runCanaryContext, stage, STAGE_BEFORE));
    }

    return stages;
  }

  private static long longFromMap(Map<String, Object> map, String key) {
    return longFromMap(map, key, 0L);
  }

  private static long longFromMap(Map<String, Object> map, String key, long defaultValue) {
    return Optional.ofNullable(map.get(key))
      .map(Object::toString)
      .map(Strings::emptyToNull)
      .map(Long::parseLong)
      .orElse(defaultValue);
  }

  static Object deepCopy(Object sourceObj) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {

      oos.writeObject(sourceObj);
      oos.flush();

      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);

      return ois.readObject();
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException(e); // TODO: better class
    }
  }

  @Override
  public String getType() {
    return "kayentaCanary";
  }

  private static <K, V> Map<K, V> map(Consumer<Map<K, V>> closure) {
    Map<K, V> map = new HashMap<>();
    closure.accept(map);
    return map;
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
