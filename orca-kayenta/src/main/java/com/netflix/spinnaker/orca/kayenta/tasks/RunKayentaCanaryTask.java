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
import com.netflix.spinnaker.orca.kayenta.KayentaService.CanaryExecutionRequest;
import com.netflix.spinnaker.orca.kayenta.model.RunCanaryContext;
import com.netflix.spinnaker.orca.pipeline.model.Stage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Nonnull;

import static com.netflix.spinnaker.orca.ExecutionStatus.SUCCEEDED;
import static java.util.Collections.singletonMap;

@Component
public class RunKayentaCanaryTask implements Task {

  private final Logger log = LoggerFactory.getLogger(getClass());

  private final KayentaService kayentaService;

  @Autowired
  public RunKayentaCanaryTask(KayentaService kayentaService) {this.kayentaService = kayentaService;}

  @Override
  public TaskResult execute(@Nonnull Stage stage) {
    RunCanaryContext context = stage.mapTo(RunCanaryContext.class);
    String canaryPipelineExecutionId = (String) kayentaService.create(
      context.canaryConfigId,
      stage.getExecution().getApplication(),
      stage.getExecution().getId(),
      context.metricsAccountName,
      context.storageAccountName /* configurationAccountName */, // TODO(duftler): Propagate configurationAccountName properly.
      context.storageAccountName,
      new CanaryExecutionRequest(context.scopes, context.scoreThresholds)
    ).get("canaryExecutionId");

    return new TaskResult(SUCCEEDED, singletonMap("canaryPipelineExecutionId", canaryPipelineExecutionId));
  }
}
