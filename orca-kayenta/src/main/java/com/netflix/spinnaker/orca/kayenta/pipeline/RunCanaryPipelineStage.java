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

import com.netflix.spinnaker.orca.CancellableStage;
import com.netflix.spinnaker.orca.kayenta.KayentaService;
import com.netflix.spinnaker.orca.kayenta.tasks.MonitorKayentaCanaryTask;
import com.netflix.spinnaker.orca.kayenta.tasks.RunKayentaCanaryTask;
import com.netflix.spinnaker.orca.pipeline.StageDefinitionBuilder;
import com.netflix.spinnaker.orca.pipeline.TaskNode;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;

@Component
public class RunCanaryPipelineStage implements StageDefinitionBuilder, CancellableStage {

  private final Logger log = LoggerFactory.getLogger(getClass());

  public static final String STAGE_TYPE = "runCanary";

  private final KayentaService kayentaService;

  @Autowired
  public RunCanaryPipelineStage(KayentaService kayentaService) {this.kayentaService = kayentaService;}

  @Override
  public void taskGraph(
    @Nonnull Stage stage,
    @Nonnull TaskNode.Builder builder
  ) {
    builder
      .withTask("runCanary", RunKayentaCanaryTask.class)
      .withTask("monitorCanary", MonitorKayentaCanaryTask.class);
  }

  @Override
  public @Nonnull String getType() {
    return STAGE_TYPE;
  }

  @Override
  public CancellableStage.Result cancel(Stage stage) {
    Map<String, Object> context = stage.getContext();
    String canaryPipelineExecutionId = (String) context.get("canaryPipelineExecutionId");

    if (canaryPipelineExecutionId != null) {
      log.info(format("Cancelling stage (stageId: %s: executionId: %s, canaryPipelineExecutionId: %s, context: %s)", stage.getId(), stage.getExecution().getId(), canaryPipelineExecutionId, stage.getContext()));

      try {
        kayentaService.cancelPipelineExecution(canaryPipelineExecutionId, "");
      } catch (Exception e) {
        log.error(format("Failed to cancel stage (stageId: %s, executionId: %s), e: %s", stage.getId(), stage.getExecution().getId(), e.getMessage()), e);
      }
    } else {
      log.info(format("Not cancelling stage (stageId: %s: executionId: %s, context: %s) since no canary pipeline execution id exists", stage.getId(), stage.getExecution().getId(), stage.getContext()));
    }

    return new CancellableStage.Result(stage, emptyMap());
  }
}
