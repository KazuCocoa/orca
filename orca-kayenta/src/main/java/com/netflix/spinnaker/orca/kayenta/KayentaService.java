package com.netflix.spinnaker.orca.kayenta;

import com.netflix.spinnaker.orca.kayenta.model.CanaryScope;
import com.netflix.spinnaker.orca.kayenta.model.Thresholds;
import retrofit.http.*;

import java.util.Map;

public interface KayentaService {

  @POST("/canary/{canaryConfigId}")
  Map create(
    @Path("canaryConfigId") String canaryConfigId,
    @Query("application") String application,
    @Query("parentPipelineExecutionId") String parentPipelineExecutionId,
    @Query("metricsAccountName") String metricsAccountName,
    @Query("configurationAccountName") String configurationAccountName,
    @Query("storageAccountName") String storageAccountName,
    @Body CanaryExecutionRequest canaryExecutionRequest
  );

  @GET("/canary/{canaryExecutionId}")
  Map getCanaryResults(
    @Query("storageAccountName") String storageAccountName,
    @Path("canaryExecutionId") String canaryExecutionId
  );

  @PUT("/pipelines/{executionId}/cancel")
  Map cancelPipelineExecution(
    @Path("executionId") String executionId,
    @Body String ignored
  );

  class CanaryExecutionRequest {
    public CanaryExecutionRequest(Map<String, Map<String, CanaryScope>> scopes, Thresholds thresholds) {
      this.scopes = scopes;
      this.thresholds = thresholds;
    }

    Map<String, Map<String, CanaryScope>> scopes;
    Thresholds thresholds;
  }
}
