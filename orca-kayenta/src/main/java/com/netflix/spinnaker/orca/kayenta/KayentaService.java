package com.netflix.spinnaker.orca.kayenta;

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
    @Body Map<String, Object> canaryExecutionRequest);

  @GET("/canary/{canaryExecutionId}")
  Map getCanaryResults(
    @Query("storageAccountName") String storageAccountName,
    @Path("canaryExecutionId") String canaryExecutionId);

  @PUT("/pipelines/{executionId}/cancel")
  Map cancelPipelineExecution(
    @Path("executionId") String executionId,
    @Body String ignored
  );
}
