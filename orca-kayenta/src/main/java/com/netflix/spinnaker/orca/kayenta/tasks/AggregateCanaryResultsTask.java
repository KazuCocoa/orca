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
import com.netflix.spinnaker.orca.kayenta.pipeline.RunCanaryPipelineStage;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.netflix.spinnaker.orca.ExecutionStatus.SUCCEEDED;
import static com.netflix.spinnaker.orca.ExecutionStatus.TERMINAL;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@Component public class AggregateCanaryResultsTask implements Task {

  private final Logger log = LoggerFactory.getLogger(getClass());

  @Override
  public TaskResult execute(@Nonnull Stage stage) {
    Map<String, Number> scoreThresholds = (Map<String, Number>) ((Map<String, Object>) stage.getContext().get("canaryConfig")).get("scoreThresholds");
    List<Stage> runCanaryStages = stage.getExecution().getStages().stream().filter(it -> it.getType().equals(RunCanaryPipelineStage.STAGE_TYPE)).collect(toList());
    List<Double> runCanaryScores = runCanaryStages.stream().map(it -> (Number) it.getContext().get("canaryScore")).map(Number::doubleValue).collect(toList());
    double finalCanaryScore = runCanaryScores.get(runCanaryScores.size() - 1);

    Double marginal = Optional.ofNullable(scoreThresholds.get("marginal")).map(Number::doubleValue).orElse(null);
    Double pass = Optional.ofNullable(scoreThresholds.get("pass")).map(Number::doubleValue).orElse(null);
    if (marginal == null && pass == null) {
      return new TaskResult(SUCCEEDED, mapOf(entry("canaryScores", runCanaryScores),
        entry("canaryScoreMessage", "No score thresholds were specified.")));
    } else if (marginal != null && finalCanaryScore <= marginal) {
      return new TaskResult(TERMINAL, mapOf(entry("canaryScores", runCanaryScores),
        entry("canaryScoreMessage", format("Final canary score %s is not above the marginal score threshold.", finalCanaryScore))));
    } else if (pass == null) {
      return new TaskResult(SUCCEEDED, mapOf(entry("canaryScores", runCanaryScores),
        entry("canaryScoreMessage", "No pass score threshold was specified.")));
    } else if (finalCanaryScore < pass) {
      return new TaskResult(TERMINAL, mapOf(entry("canaryScores", runCanaryScores),
        entry("canaryScoreMessage", format("Final canary score %s is below the pass score threshold.", finalCanaryScore))));
    } else {
      return new TaskResult(SUCCEEDED, mapOf(entry("canaryScores", runCanaryScores),
        entry("canaryScoreMessage", format("Final canary score %s met or exceeded the pass score threshold.", finalCanaryScore))));
    }
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
