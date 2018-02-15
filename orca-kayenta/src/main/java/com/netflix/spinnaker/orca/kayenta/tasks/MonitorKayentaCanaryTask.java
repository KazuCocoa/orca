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

import com.netflix.spinnaker.orca.ExecutionStatus;
import com.netflix.spinnaker.orca.OverridableTimeoutRetryableTask;
import com.netflix.spinnaker.orca.TaskResult;
import com.netflix.spinnaker.orca.kayenta.KayentaService;
import com.netflix.spinnaker.orca.kayenta.model.Thresholds;
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
import java.util.concurrent.TimeUnit;

import static com.netflix.spinnaker.orca.ExecutionStatus.*;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;

@Component
public class MonitorKayentaCanaryTask implements OverridableTimeoutRetryableTask {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Override public long getBackoffPeriod() {
    return 1000L;
  }

  @Override public long getTimeout() {
    return TimeUnit.HOURS.toMillis(12);
  }

  private final KayentaService kayentaService;

  @Autowired
  public MonitorKayentaCanaryTask(KayentaService kayentaService) {this.kayentaService = kayentaService;}

  @Override
  public TaskResult execute(@Nonnull Stage stage) {
    Map<String, Object> context = stage.getContext();
    String canaryPipelineExecutionId = (String) context.get("canaryPipelineExecutionId");
    String storageAccountName = (String) context.get("storageAccountName");
    Map canaryResults = kayentaService.getCanaryResults(storageAccountName, canaryPipelineExecutionId);
    ExecutionStatus status = ExecutionStatus.valueOf(canaryResults.get("status").toString().toUpperCase());

    if (status == SUCCEEDED) {
      Thresholds scoreThresholds = (Thresholds) context.get("scoreThresholds");
      // TODO: for the love of god can this be an actual type?
      Map<String, Object> result = (Map<String, Object>) canaryResults.get("result");
      double canaryScore = ((Map<String, Map<String, Double>>) result.get("judgeResult")).get("score").get("score");
      long lastUpdatedMs = (long) canaryResults.get("endTimeMillis");
      String lastUpdatedIso = (String) canaryResults.get("endTimeIso");
      String durationString = (String) result.get("canaryDuration");

      if (scoreThresholds.marginal == null && scoreThresholds.pass == null) {
        return new TaskResult(SUCCEEDED, mapOf(
          entry("canaryPipelineStatus", SUCCEEDED),
          entry("lastUpdated", lastUpdatedMs),
          entry("lastUpdatedIso", lastUpdatedIso),
          entry("durationString", durationString),
          entry("canaryScore", canaryScore),
          entry("canaryScoreMessage", "No score thresholds were specified.")
        ));
      } else if (scoreThresholds.marginal == null) {
        return new TaskResult(SUCCEEDED, mapOf(
          entry("canaryPipelineStatus", SUCCEEDED),
          entry("lastUpdated", lastUpdatedMs),
          entry("lastUpdatedIso", lastUpdatedIso),
          entry("durationString", durationString),
          entry("canaryScore", canaryScore),
          entry("canaryScoreMessage", "No marginal score threshold was specified.")
        ));
      } else if (canaryScore <= scoreThresholds.marginal) {
        return new TaskResult(TERMINAL, mapOf(
          entry("canaryPipelineStatus", SUCCEEDED),
          entry("lastUpdated", lastUpdatedMs),
          entry("lastUpdatedIso", lastUpdatedIso),
          entry("durationString", durationString),
          entry("canaryScore", canaryScore),
          entry("canaryScoreMessage", "Canary score is not above the marginal score threshold.")
        ));
      } else {
        return new TaskResult(SUCCEEDED, mapOf(
          entry("canaryPipelineStatus", SUCCEEDED),
          entry("lastUpdated", lastUpdatedMs),
          entry("lastUpdatedIso", lastUpdatedIso),
          entry("durationString", durationString),
          entry("canaryScore", canaryScore)
        ));
      }
    }

    if (status.isHalt()) {
      Map<String, Object> stageOutputs = singletonMap("canaryPipelineStatus", status);

      if (canaryResults.get("exception") != null) {
        stageOutputs.put("exception", canaryResults.get("exception"));
      } else if (status == CANCELED) {
        stageOutputs.put("exception", singletonMap("details", singletonMap("errors", singletonList("Canary execution was canceled."))));
      }

      // Indicates a failure of some sort.
      return new TaskResult(TERMINAL, stageOutputs);
    }

    return new TaskResult(RUNNING, singletonMap("canaryPipelineStatus", status));
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
