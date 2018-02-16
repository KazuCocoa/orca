package com.netflix.spinnaker.orca.kayenta

import com.netflix.spinnaker.orca.kayenta.model.CanaryScope
import com.netflix.spinnaker.orca.kayenta.model.Thresholds
import retrofit.http.*

interface KayentaService {

  @POST("/canary/{canaryConfigId}")
  fun create(
    @Path("canaryConfigId") canaryConfigId: String,
    @Query("application") application: String,
    @Query("parentPipelineExecutionId") parentPipelineExecutionId: String,
    @Query("metricsAccountName") metricsAccountName: String,
    @Query("configurationAccountName") configurationAccountName: String,
    @Query("storageAccountName") storageAccountName: String,
    @Body canaryExecutionRequest: CanaryExecutionRequest
  ): Map<*, *>

  @GET("/canary/{canaryExecutionId}")
  fun getCanaryResults(
    @Query("storageAccountName") storageAccountName: String,
    @Path("canaryExecutionId") canaryExecutionId: String
  ): Map<*, *>

  @PUT("/pipelines/{executionId}/cancel")
  fun cancelPipelineExecution(
    @Path("executionId") executionId: String,
    @Body ignored: String
  ): Map<*, *>

  data class CanaryExecutionRequest(
    var scopes: Map<String, Map<String, CanaryScope>>,
    var thresholds: Thresholds
  )
}
