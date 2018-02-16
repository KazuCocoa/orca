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

package com.netflix.spinnaker.orca.kayenta.pipeline

import com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS
import com.netflix.spinnaker.orca.jackson.OrcaObjectMapper
import com.netflix.spinnaker.orca.kayenta.model.CanaryConfig
import com.netflix.spinnaker.orca.kayenta.model.CanaryConfigScope
import com.netflix.spinnaker.orca.kayenta.model.CanaryScope
import com.netflix.spinnaker.orca.kayenta.model.RunCanaryContext
import com.netflix.spinnaker.orca.kayenta.tasks.AggregateCanaryResultsTask
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder.newStage
import com.netflix.spinnaker.orca.pipeline.TaskNode.Builder
import com.netflix.spinnaker.orca.pipeline.WaitStage
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.model.SyntheticStageOwner.STAGE_BEFORE
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit.MINUTES
import java.util.*
import java.util.Collections.emptyMap
import java.util.Collections.singletonMap

@Component
class KayentaCanaryStage(
  private val clock: Clock,
  private val waitStage: WaitStage)
  : StageDefinitionBuilder {

  private val mapper = OrcaObjectMapper.newInstance()
    .disable(WRITE_DATES_AS_TIMESTAMPS) // we want Instant serialized as ISO string

  override fun taskGraph(stage: Stage, builder: Builder) {
    builder.withTask("aggregateCanaryResults", AggregateCanaryResultsTask::class.java)
  }

  override fun aroundStages(stage: Stage): List<Stage> {
    val canaryConfig = stage.mapTo("/canaryConfig", CanaryConfig::class.java)

    if (canaryConfig.scopes?.isEmpty() == true) {
      throw IllegalArgumentException("Canary stage configuration must contain at least one scope.")
    }

    // Using time boundaries from just the first scope since it doesn't really make sense for each scope to have different boundaries.
    // TODO(duftler): Add validation to log warning when time boundaries differ across scopes.
    val firstScope = canaryConfig.scopes!![0]

    val lifetimeMinutes: Int
    if (firstScope.endTime != null) {
      lifetimeMinutes = Optional.ofNullable(firstScope.startTime).orElseGet { Instant.now(clock) }.until(firstScope.endTime, MINUTES).toInt()
    } else if (canaryConfig.lifetimeHours != null) {
      lifetimeMinutes = Duration.ofHours(canaryConfig.lifetimeHours.toLong()).toMinutes().toInt()
    } else {
      throw IllegalArgumentException("Canary stage configuration must include either `endTime` or `lifetimeHours`.")
    }

    var canaryAnalysisIntervalMins = canaryConfig.canaryAnalysisIntervalMins
      ?: lifetimeMinutes
    if (canaryAnalysisIntervalMins == 0 || canaryAnalysisIntervalMins > lifetimeMinutes) {
      canaryAnalysisIntervalMins = lifetimeMinutes
    }

    val numIntervals = lifetimeMinutes / canaryAnalysisIntervalMins
    val stages = ArrayList<Stage>()

    if (canaryConfig.beginCanaryAnalysisAfterMins > 0L) {
      stages.add(newStage(
        stage.execution,
        waitStage.type,
        "Warmup Wait",
        singletonMap<String, Any>("waitTime", Duration.ofMinutes(canaryConfig.beginCanaryAnalysisAfterMins.toLong()).seconds),
        stage,
        STAGE_BEFORE
      ))
    }

    for (i in 1..numIntervals) {
      // If an end time was explicitly specified, we don't need to synchronize the execution of the canary pipeline with the real time.
      if (firstScope.endTime == null) {
        stages.add(newStage(
          stage.execution,
          waitStage.type,
          "Interval Wait #" + i,
          singletonMap<String, Any>("waitTime", Duration.ofMinutes(canaryAnalysisIntervalMins.toLong()).seconds),
          stage,
          STAGE_BEFORE
        ))
      }

      val runCanaryContext = RunCanaryContext(
        canaryConfig.metricsAccountName,
        canaryConfig.storageAccountName,
        canaryConfig.canaryConfigId,
        buildRequestScopes(canaryConfig.scopes!!),
        canaryConfig.scoreThresholds
      )

      for ((_, contextScope) in runCanaryContext.scopes) {
        var start: Instant
        val end: Instant

        if (firstScope.endTime == null) {
          start = Optional.ofNullable(firstScope.startTime).orElseGet { Instant.now(clock) }.plus(canaryConfig.beginCanaryAnalysisAfterMins.toLong(), MINUTES)
          end = Optional.ofNullable(firstScope.startTime).orElseGet { Instant.now(clock) }.plus((canaryConfig.beginCanaryAnalysisAfterMins + i * canaryAnalysisIntervalMins).toLong(), MINUTES)
        } else {
          start = Optional.ofNullable(firstScope.startTime).orElseGet { Instant.now(clock) }
          end = Optional.ofNullable(firstScope.startTime).orElseGet { Instant.now(clock) }.plus((i * canaryAnalysisIntervalMins).toLong(), MINUTES)
        }

        if (canaryConfig.lookbackMins > 0) {
          start = end.minus(canaryConfig.lookbackMins.toLong(), MINUTES)
        }

        val controlScope = contextScope["controlScope"]!!
        controlScope.start = start
        controlScope.end = end
        val experimentScope = contextScope["experimentScope"]!!
        experimentScope.start = start
        experimentScope.end = end
      }

      stages.add(newStage(
        stage.execution,
        RunCanaryPipelineStage.STAGE_TYPE,
        "Run Canary #" + i,
        mapper.convertValue(runCanaryContext, Map::class.java) as Map<String, Any>,
        stage,
        STAGE_BEFORE
      ))
    }

    return stages
  }

  fun buildRequestScopes(configScopes: List<CanaryConfigScope>): Map<String, Map<String, CanaryScope>> {
    val requestScopes = HashMap<String, Map<String, CanaryScope>>()
    configScopes.forEach { scope ->
      val controlScope = CanaryScope(
        scope.controlScope,
        scope.controlRegion,
        scope.startTime,
        scope.endTime,
        scope.step ?: "60",
        scope.extendedScopeParams ?: emptyMap()
      )
      val experimentScope = CanaryScope(
        scope.experimentScope,
        scope.experimentRegion,
        scope.startTime,
        scope.endTime,
        scope.step ?: "60",
        scope.extendedScopeParams ?: emptyMap()
      )

      requestScopes[scope.scopeName ?: "default"] = mapOf(
        "controlScope" to controlScope,
        "experimentScope" to experimentScope
      )
    }
    return requestScopes
  }

  override fun getType(): String {
    return "kayentaCanary"
  }
}
