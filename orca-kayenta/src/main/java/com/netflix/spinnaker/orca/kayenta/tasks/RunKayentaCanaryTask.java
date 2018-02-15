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

package com.netflix.spinnaker.orca.kayenta.tasks;

import com.netflix.spinnaker.orca.Task;
import com.netflix.spinnaker.orca.TaskResult;
import com.netflix.spinnaker.orca.kayenta.KayentaService;
import com.netflix.spinnaker.orca.pipeline.model.Execution;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static com.netflix.spinnaker.orca.ExecutionStatus.SUCCEEDED;

@Component
public class RunKayentaCanaryTask implements Task {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final KayentaService kayentaService;

  @Autowired
  public RunKayentaCanaryTask(KayentaService kayentaService) {this.kayentaService = kayentaService;}

  @Override
  public TaskResult execute(@Nonnull Stage stage) {
    Map<String, Object> context = stage.getContext();
    String metricsAccountName = (String) context.get("metricsAccountName");
    String storageAccountName = (String) context.get("storageAccountName");
    String canaryConfigId = (String) context.get("canaryConfigId");
    Map<String, Map> scopes = (Map<String, Map>) context.get("scopes");
    Map<String, String> scoreThresholds = (Map<String, String>) context.get("scoreThresholds");
    Map<String, Object> canaryExecutionRequest = mapOf(
      entry("scopes", scopes),
      entry("thresholds", mapOf(
        entry("pass", scoreThresholds.get("pass")),
        entry("marginal", scoreThresholds.get("marginal"))
        )
      )
    );
    Execution execution = stage.getExecution();
    String canaryPipelineExecutionId = (String) kayentaService.create(canaryConfigId,
      execution.getApplication(),
      execution.getId(),
      metricsAccountName,
      storageAccountName /* configurationAccountName */, // TODO(duftler): Propagate configurationAccountName properly.
      storageAccountName,
      canaryExecutionRequest
    ).get("canaryExecutionId");

    return new TaskResult(SUCCEEDED, mapOf(entry("canaryPipelineExecutionId",
      canaryPipelineExecutionId)));
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
