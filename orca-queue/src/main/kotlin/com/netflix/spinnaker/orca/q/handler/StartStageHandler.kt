/*
 * Copyright 2017 Netflix, Inc.
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

package com.netflix.spinnaker.orca.q.handler

import com.fasterxml.jackson.databind.ObjectMapper
import com.netflix.spectator.api.Registry
import com.netflix.spinnaker.orca.ExecutionStatus.NOT_STARTED
import com.netflix.spinnaker.orca.ExecutionStatus.RUNNING
import com.netflix.spinnaker.orca.events.StageStarted
import com.netflix.spinnaker.orca.exceptions.ExceptionHandler
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilderFactory
import com.netflix.spinnaker.orca.pipeline.expressions.PipelineExpressionEvaluator
import com.netflix.spinnaker.orca.pipeline.model.Execution.ExecutionType.PIPELINE
import com.netflix.spinnaker.orca.pipeline.model.OptionalStageSupport
import com.netflix.spinnaker.orca.pipeline.model.Stage
import com.netflix.spinnaker.orca.pipeline.persistence.ExecutionRepository
import com.netflix.spinnaker.orca.pipeline.util.ContextParameterProcessor
import com.netflix.spinnaker.orca.pipeline.util.StageNavigator
import com.netflix.spinnaker.orca.q.CompleteExecution
import com.netflix.spinnaker.orca.q.CompleteStage
import com.netflix.spinnaker.orca.q.SkipStage
import com.netflix.spinnaker.orca.q.StartStage
import com.netflix.spinnaker.orca.q.StartTask
import com.netflix.spinnaker.orca.q.allUpstreamStagesComplete
import com.netflix.spinnaker.orca.q.anyUpstreamStagesFailed
import com.netflix.spinnaker.orca.q.buildSyntheticStages
import com.netflix.spinnaker.orca.q.buildTasks
import com.netflix.spinnaker.orca.q.firstAfterStages
import com.netflix.spinnaker.orca.q.firstBeforeStages
import com.netflix.spinnaker.orca.q.firstTask
import com.netflix.spinnaker.q.AttemptsAttribute
import com.netflix.spinnaker.q.MaxAttemptsAttribute
import com.netflix.spinnaker.q.Queue
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.ApplicationEventPublisher
import org.springframework.stereotype.Component
import java.time.Clock
import java.time.Duration
import kotlin.collections.set

@Component
class StartStageHandler(
  override val queue: Queue,
  override val repository: ExecutionRepository,
  override val stageNavigator: StageNavigator,
  override val stageDefinitionBuilderFactory: StageDefinitionBuilderFactory,
  override val contextParameterProcessor: ContextParameterProcessor,
  @Qualifier("queueEventPublisher") private val publisher: ApplicationEventPublisher,
  private val exceptionHandlers: List<ExceptionHandler>,
  @Qualifier("mapper") private val objectMapper: ObjectMapper,
  private val clock: Clock,
  private val registry: Registry,
  @Value("\${queue.retry.delay.ms:15000}") retryDelayMs: Long
) : OrcaMessageHandler<StartStage>, StageBuilderAware, ExpressionAware, AuthenticationAware {

  private val retryDelay = Duration.ofMillis(retryDelayMs)

  override fun handle(message: StartStage) {
    message.withStage { stage ->
      val mergedContextStage = stage.withMergedContext()
      if (mergedContextStage.anyUpstreamStagesFailed()) {
        // this only happens in restart scenarios
        log.warn("Tried to start stage ${mergedContextStage.id} but something upstream had failed (executionId: ${message.executionId})")
        queue.push(CompleteExecution(message))
      } else if (mergedContextStage.allUpstreamStagesComplete()) {
        if (mergedContextStage.status != NOT_STARTED) {
          log.warn("Ignoring $message as stage is already ${mergedContextStage.status}")
        } else if (mergedContextStage.shouldSkip()) {
          queue.push(SkipStage(message))
        } else {
          try {
            mergedContextStage.withAuth {
              mergedContextStage.withMergedContext().plan()
            }

            mergedContextStage.status = RUNNING
            mergedContextStage.startTime = clock.millis()
            repository.storeStage(mergedContextStage)

            mergedContextStage.start()

            publisher.publishEvent(StageStarted(this, mergedContextStage))
            trackResult(mergedContextStage)
          } catch(e: Exception) {
            val exceptionDetails = exceptionHandlers.shouldRetry(e, mergedContextStage.name)
            if (exceptionDetails?.shouldRetry == true) {
              val attempts = message.getAttribute<AttemptsAttribute>()?.attempts ?: 0
              log.warn("Error planning ${mergedContextStage.type} stage for ${message.executionType}[${message.executionId}] (attempts: $attempts)")

              message.setAttribute(MaxAttemptsAttribute(40))
              queue.push(message, retryDelay)
            } else {
              log.error("Error running ${mergedContextStage.type} stage for ${message.executionType}[${message.executionId}]", e)
              mergedContextStage.context["exception"] = exceptionDetails
              repository.storeStage(mergedContextStage)
              queue.push(CompleteStage(message))
            }
          }
        }
      } else {
        log.warn("Re-queuing $message as upstream stages are not yet complete")
        queue.push(message, retryDelay)
      }
    }
  }

  private fun trackResult(stage: Stage) {
    // We only want to record invocations of parent-level stages; not synthetics
    if (stage.parentStageId != null) {
      return
    }

    val id = registry.createId("stage.invocations")
      .withTag("type", stage.type)
      .withTag("application", stage.execution.application)
      .let { id ->
        // TODO rz - Need to check synthetics for their cloudProvider.
        stage.context["cloudProvider"]?.let {
          id.withTag("cloudProvider", it.toString())
        } ?: id
      }
    registry.counter(id).increment()
  }

  override val messageType = StartStage::class.java

  private fun Stage.plan() {
    builder().let { builder ->
      builder.buildTasks(this)
      builder.buildSyntheticStages(this) { it: Stage ->
        repository.addStage(it)
      }
    }
  }

  private fun Stage.start() {
    val beforeStages = firstBeforeStages()
    if (beforeStages.isEmpty()) {
      val task = firstTask()
      if (task == null) {
        val afterStages = firstAfterStages()
        if (afterStages.isEmpty()) {
          queue.push(CompleteStage(this))
        } else {
          afterStages.forEach {
            queue.push(StartStage(it))
          }
        }
      } else {
        queue.push(StartTask(this, task.id))
      }
    } else {
      beforeStages.forEach {
        queue.push(StartStage(it))
      }
    }
  }

  private fun Stage.shouldSkip(): Boolean {
    if (this.execution.type != PIPELINE) {
      return false
    }

    val clonedContext = objectMapper.convertValue(this.context, Map::class.java) as Map<String, Any>
    val clonedStage = Stage(this.execution, this.type, clonedContext).also {
      it.refId = refId
      it.requisiteStageRefIds = requisiteStageRefIds
      it.syntheticStageOwner = syntheticStageOwner
      it.parentStageId = parentStageId
    }
    if (clonedStage.context.containsKey(PipelineExpressionEvaluator.SUMMARY)) {
      this.context.put(PipelineExpressionEvaluator.SUMMARY, clonedStage.context[PipelineExpressionEvaluator.SUMMARY])
    }

    return OptionalStageSupport.isOptional(clonedStage.withMergedContext(), contextParameterProcessor)
  }
}
