/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes;

import io.fabric8.volcano.client.VolcanoClient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerDriverConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.KubernetesTaskManagerSpecification;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.KubernetesCustomizedScheduler;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient.FlinkVolcanoClient;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesTooOldResourceVersionException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.util.ResourceManagerUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nullable;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Implementation of {@link ResourceManagerDriver} for Kubernetes deployment. */
public class KubernetesResourceManagerDriver
        extends AbstractResourceManagerDriver<KubernetesWorkerNode> {

    /** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
    private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

    private final String clusterId;

    private final String webInterfaceUrl;

    private final FlinkKubeClient flinkKubeClient;

    /** Request resource futures, keyed by pod names. */
    private final Map<String, CompletableFuture<KubernetesWorkerNode>> requestResourceFutures;

    /** When ResourceManager failover, the max attempt should recover. */
    private long currentMaxAttemptId = 0;

    /** Current max pod index. When creating a new pod, it should increase one. */
    private long currentMaxPodId = 0;

    private Optional<KubernetesWatch> podsWatchOpt;

    private volatile boolean running;

    private FlinkPod taskManagerPodTemplate;

    public KubernetesResourceManagerDriver(
            Configuration flinkConfig,
            FlinkKubeClient flinkKubeClient,
            KubernetesResourceManagerDriverConfiguration configuration) {
        super(flinkConfig, GlobalConfiguration.loadConfiguration());
        this.clusterId = Preconditions.checkNotNull(configuration.getClusterId());
        this.webInterfaceUrl = configuration.getWebInterfaceUrl();
        this.flinkKubeClient = Preconditions.checkNotNull(flinkKubeClient);
        this.requestResourceFutures = new HashMap<>();
        this.running = false;
    }

    // ------------------------------------------------------------------------
    //  ResourceManagerDriver
    // ------------------------------------------------------------------------

    @Override
    protected void initializeInternal() throws Exception {
        podsWatchOpt = watchTaskManagerPods();
        final File podTemplateFile = KubernetesUtils.getTaskManagerPodTemplateFileInPod();
        if (podTemplateFile.exists()) {
            taskManagerPodTemplate =
                    KubernetesUtils.loadPodFromTemplateFile(
                            flinkKubeClient, podTemplateFile, Constants.MAIN_CONTAINER_NAME);
        } else {
            taskManagerPodTemplate = new FlinkPod.Builder().build();
        }
        updateKubernetesServiceTargetPortIfNecessary();
        recoverWorkerNodesFromPreviousAttempts();
        this.running = true;
    }

    @Override
    public void terminate() throws Exception {
        if (!running) {
            return;
        }
        running = false;

        // shut down all components
        Exception exception = null;

        try {
            podsWatchOpt.ifPresent(KubernetesWatch::close);
        } catch (Exception e) {
            exception = e;
        }

        try {
            flinkKubeClient.close();
        } catch (Exception e) {
            exception = ExceptionUtils.firstOrSuppressed(e, exception);
        }

        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public void deregisterApplication(
            ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
        log.info(
                "Deregistering Flink Kubernetes cluster, clusterId: {}, diagnostics: {}",
                clusterId,
                optionalDiagnostics == null ? "" : optionalDiagnostics);
        flinkKubeClient.stopAndCleanupCluster(clusterId);
    }

    @Override
    public CompletableFuture<KubernetesWorkerNode> requestResource(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final KubernetesTaskManagerParameters parameters =
                createKubernetesTaskManagerParameters(taskExecutorProcessSpec);
        final KubernetesTaskManagerSpecification taskManagerSpec =
                KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(
                        taskManagerPodTemplate, parameters);
        final String podName = taskManagerSpec.getPod().getName();

        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                new CompletableFuture<>();

        requestResourceFutures.put(podName, requestResourceFuture);

        log.info(
                "Creating new TaskManager pod with name {} and resource <{},{}>.",
                podName,
                parameters.getTaskManagerMemoryMB(),
                parameters.getTaskManagerCPU());

        final CompletableFuture<Void> createPodFuture =
                flinkKubeClient.createTaskManagerPod(taskManagerSpec);

        FutureUtils.assertNoException(
                createPodFuture.handleAsync(
                        (ignore, exception) -> {
                            if (exception != null) {
                                log.warn(
                                        "Could not create pod {}, exception: {}",
                                        podName,
                                        exception);
                                CompletableFuture<KubernetesWorkerNode> future =
                                        requestResourceFutures.remove(taskManagerSpec.getPod().getName());
                                if (future != null) {
                                    future.completeExceptionally(exception);
                                }
                            } else {
                                log.info("Pod {} is created.", podName);
                            }
                            return null;
                        },
                        getMainThreadExecutor()));

        return requestResourceFuture;
    }

    @Override
    public void releaseResource(KubernetesWorkerNode worker) {
        final String podName = worker.getResourceID().toString();

        log.info("Stopping TaskManager pod {}.", podName);

        stopPod(podName);
    }

    @Override
    public void refreshAssociatedJobResources(JobID jobId) {
        log.warn(
                "[TEST] Start to refresh Job resources for JobID {}.",
                jobId.toString());
        // Check whether the K8S Customize Scheduler is enabled
        List<KubernetesPod> podList =
                flinkKubeClient.getPodsWithLabels(
                        KubernetesUtils.getTaskManagerSelectors(clusterId));

        Boolean isEnabled = Boolean.FALSE;
        String customerizedSchedulerName = null;

        for (KubernetesPod pod : podList) {
            String schedulerName = pod.getInternalResource().getSpec().getSchedulerName();
            if (!schedulerName.equals("default-scheduler")) {
                isEnabled = Boolean.TRUE;
                customerizedSchedulerName = schedulerName;
                break;
            }
        }

        // ext enabled, get customerized scheduler
        if (isEnabled) {
            if (KubernetesCustomizedScheduler.isSupportCustomizedScheduler(customerizedSchedulerName)) {
                // TODO: NEED make it more common
                VolcanoClient volcanoClient = FlinkVolcanoClient.getVolcanoClient(this.flinkConfig);
                log.warn("[TEST] Get volcano client in refresh.");
                // TODO: muiltiple threads support
                volcanoClient.podGroups().withName("pod-group-"+jobId.toString()).delete();
                log.warn("[TEST] End for clean podgroup {}", "pod-group-"+jobId.toString());
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Internal
    // ------------------------------------------------------------------------

    private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
        List<KubernetesPod> podList =
                flinkKubeClient.getPodsWithLabels(
                        KubernetesUtils.getTaskManagerSelectors(clusterId));
        final List<KubernetesWorkerNode> recoveredWorkers = new ArrayList<>();

        for (KubernetesPod pod : podList) {
            final KubernetesWorkerNode worker =
                    new KubernetesWorkerNode(new ResourceID(pod.getName()));
            final long attempt = worker.getAttempt();
            if (attempt > currentMaxAttemptId) {
                currentMaxAttemptId = attempt;
            }

            if (pod.isTerminated() || !pod.isScheduled()) {
                stopPod(pod.getName());
            } else {
                recoveredWorkers.add(worker);
            }
        }

        log.info(
                "Recovered {} pods from previous attempts, current attempt id is {}.",
                recoveredWorkers.size(),
                ++currentMaxAttemptId);

        // Should not invoke resource event handler on the main thread executor.
        // We are in the initializing thread. The main thread executor is not yet ready.
        getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
    }

    private void updateKubernetesServiceTargetPortIfNecessary() throws Exception {
        if (!KubernetesUtils.isHostNetwork(flinkConfig)) {
            return;
        }
        final int restPort =
                ResourceManagerUtils.parseRestBindPortFromWebInterfaceUrl(webInterfaceUrl);
        Preconditions.checkArgument(
                restPort > 0, "Failed to parse rest port from " + webInterfaceUrl);
        flinkKubeClient
                .updateServiceTargetPort(
                        KubernetesService.ServiceType.REST_SERVICE,
                        clusterId,
                        Constants.REST_PORT_NAME,
                        restPort)
                .get();
        if (!HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfig)) {
            flinkKubeClient
                    .updateServiceTargetPort(
                            KubernetesService.ServiceType.INTERNAL_SERVICE,
                            clusterId,
                            Constants.BLOB_SERVER_PORT_NAME,
                            Integer.parseInt(flinkConfig.getString(BlobServerOptions.PORT)))
                    .get();
            flinkKubeClient
                    .updateServiceTargetPort(
                            KubernetesService.ServiceType.INTERNAL_SERVICE,
                            clusterId,
                            Constants.JOB_MANAGER_RPC_PORT_NAME,
                            flinkConfig.getInteger(JobManagerOptions.PORT))
                    .get();
        }
    }

    private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(
            TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final String podName =
                String.format(
                        TASK_MANAGER_POD_FORMAT, clusterId, currentMaxAttemptId, ++currentMaxPodId);

        final ContaineredTaskManagerParameters taskManagerParameters =
                ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

        final Configuration taskManagerConfig = new Configuration(flinkConfig);
        taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, podName);

        final String dynamicProperties =
                BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);
        final String jvmMemOpts =
                ProcessMemoryUtils.generateJvmParametersStr(taskExecutorProcessSpec);
        return new KubernetesTaskManagerParameters(
                flinkConfig,
                podName,
                dynamicProperties,
                jvmMemOpts,
                taskManagerParameters,
                ExternalResourceUtils.getExternalResourceConfigurationKeys(
                        flinkConfig,
                        KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX),
                taskExecutorProcessSpec.getAssociatedJobs());
    }

    private void handlePodEventsInMainThread(List<KubernetesPod> pods) {
        getMainThreadExecutor()
                .execute(
                        () -> {
                            for (KubernetesPod pod : pods) {
                                if (pod.isTerminated()) {
                                    onPodTerminated(pod);
                                } else if (pod.isScheduled()) {
                                    onPodScheduled(pod);
                                }
                            }
                        });
    }

    private void onPodScheduled(KubernetesPod pod) {
        final String podName = pod.getName();
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                requestResourceFutures.remove(podName);

        if (requestResourceFuture == null) {
            log.debug("Ignore TaskManager pod that is already added: {}", podName);
            return;
        }

        log.info("Received new TaskManager pod: {}", podName);
        requestResourceFuture.complete(new KubernetesWorkerNode(new ResourceID(podName)));
    }

    private void onPodTerminated(KubernetesPod pod) {
        final String podName = pod.getName();
        log.debug("TaskManager pod {} is terminated.", podName);

        // this is a safe net, in case onModified/onDeleted/onError is
        // received before onAdded
        final CompletableFuture<KubernetesWorkerNode> requestResourceFuture =
                requestResourceFutures.remove(podName);
        if (requestResourceFuture != null) {
            log.warn("Pod {} is terminated before being scheduled.", podName);
            requestResourceFuture.completeExceptionally(new FlinkException("Pod is terminated."));
        }

        getResourceEventHandler()
                .onWorkerTerminated(new ResourceID(podName), pod.getTerminatedDiagnostics());
        stopPod(podName);
    }

    private void stopPod(String podName) {
        flinkKubeClient
                .stopPod(podName)
                .whenComplete(
                        (ignore, throwable) -> {
                            if (throwable != null) {
                                log.warn(
                                        "Could not remove TaskManager pod {}, exception: {}",
                                        podName,
                                        throwable);
                            }
                        });
    }

    private Optional<KubernetesWatch> watchTaskManagerPods() throws Exception {
        return Optional.of(
                flinkKubeClient.watchPodsAndDoCallback(
                        KubernetesUtils.getTaskManagerSelectors(clusterId),
                        new PodCallbackHandlerImpl()));
    }

    // ------------------------------------------------------------------------
    //  FlinkKubeClient.WatchCallbackHandler
    // ------------------------------------------------------------------------

    private class PodCallbackHandlerImpl
            implements FlinkKubeClient.WatchCallbackHandler<KubernetesPod> {
        @Override
        public void onAdded(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods);
        }

        @Override
        public void onModified(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods);
        }

        @Override
        public void onDeleted(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods);
        }

        @Override
        public void onError(List<KubernetesPod> pods) {
            handlePodEventsInMainThread(pods);
        }

        @Override
        public void handleError(Throwable throwable) {
            if (throwable instanceof KubernetesTooOldResourceVersionException) {
                getMainThreadExecutor()
                        .execute(
                                () -> {
                                    if (running) {
                                        podsWatchOpt.ifPresent(KubernetesWatch::close);
                                        log.info("Creating a new watch on TaskManager pods.");
                                        try {
                                            podsWatchOpt = watchTaskManagerPods();
                                        } catch (Exception e) {
                                            getResourceEventHandler().onError(e);
                                        }
                                    }
                                });
            } else {
                getResourceEventHandler().onError(throwable);
            }
        }
    }
}
