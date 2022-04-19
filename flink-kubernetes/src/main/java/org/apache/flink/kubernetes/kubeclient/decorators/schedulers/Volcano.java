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

package org.apache.flink.kubernetes.kubeclient.decorators.schedulers;

import io.fabric8.volcano.scheduling.v1beta1.PodGroup;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** TODO. */
public class Volcano extends KubernetesCustomizedScheduler {

    private static final String QUEUE_PREFIX = "scheduling.volcano.sh/queue-name";
    private static final String PODGROUP_PREFIX = "scheduling.k8s.io/group-name";

    public static Map<String, Class> getResourceClassMap() {
        return resourceClassMap;
    }

    public static final Map<String, Class> resourceClassMap = Collections.singletonMap(
            "podgroup", PodGroup.class);

    @Override
    public Object getJobId() {
        return jobId;
    }

    private Object jobId = null;
    private String queue;
    private String minMemberPerJob;
    private String minMemberKey = "minmember";
    private String minCpuPerJob;
    private String minCpuKey = "mincpu";

    private String minMemoryPerJob;
    private String minMemoryKey = "minmemory";
    private String priorityClassName;
    private String priorityClassKey = "priorityclass";
    private Map<String, String> annotations;
    private String jobPrefix = "pod-group-";
    private final String namespace;

    public Volcano(
            AbstractKubernetesParameters kubernetesComponentConf, Configuration flinkConfig) {
        super(kubernetesComponentConf, flinkConfig);
        this.namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
        String deployment = this.flinkConfig.getString(DeploymentOptions.TARGET);
        // check whether the deployment is session mode.
        if (KubernetesDeploymentTarget.SESSION.getName().equals(deployment)) {
            if (kubernetesComponentConf.getAssociatedJobs() != null
                    && kubernetesComponentConf.getAssociatedJobs().size() >= 1) {
                this.jobId = kubernetesComponentConf.getAssociatedJobs().toArray()[0];
            }
        } else {
            // else including application mode and its embedded mode for its taskmanager pods.
            this.jobId = this.flinkConfig.getOptional(KubernetesConfigOptions.CLUSTER_ID).get();
        }
    }

    @Override
    public CustomizedScheduler settingPropertyIntoScheduler(List<Map<String, String>> mapList) {

        this.annotations = mapList.get(0);
        this.queue = this.annotations.getOrDefault(QUEUE_PREFIX, null);
        if (!this.annotations.containsKey(PODGROUP_PREFIX) && this.jobId != null) {
            if (this.annotations.isEmpty()) {
                this.annotations =
                        Collections.singletonMap(PODGROUP_PREFIX, this.jobPrefix + this.jobId);
            } else {
                this.annotations.put(PODGROUP_PREFIX, this.jobPrefix + this.jobId);
            }
        }

        Map<String, String> configs = mapList.get(1);

        for (Map.Entry<String, String> stringStringEntry : configs.entrySet()) {
            if (stringStringEntry.getKey().toLowerCase().equals(minMemberKey)) {
                this.minMemberPerJob = stringStringEntry.getValue();
            } else if (stringStringEntry.getKey().toLowerCase().equals(minCpuKey)) {
                this.minCpuPerJob = stringStringEntry.getValue();
            } else if (stringStringEntry.getKey().toLowerCase().equals(minMemoryKey)) {
                this.minMemoryPerJob = stringStringEntry.getValue();
            } else if (stringStringEntry.getKey().toLowerCase().equals(priorityClassKey)) {
                this.priorityClassName = stringStringEntry.getValue();
            }
        }

        return this;
    }

    @Override
    public HasMetadata prepareRequestResources() {
        HashMap<String, Quantity> minResources = new HashMap<>();
        if (this.minCpuPerJob != null) {
            minResources.put(Constants.RESOURCE_NAME_CPU, new Quantity(this.minCpuPerJob));
        }
        if (this.minMemoryPerJob != null) {
            minResources.put(
                    Constants.RESOURCE_NAME_MEMORY,
                    new Quantity(this.minMemoryPerJob, Constants.RESOURCE_UNIT_MB));
        }

        if (this.jobId != null) {
            PodGroupBuilder podGroupBuilder = new PodGroupBuilder();
            podGroupBuilder
                    .editOrNewMetadata()
                    .withName(this.jobPrefix + this.jobId.toString())
                    .withNamespace(this.namespace)
                    .endMetadata()
                    .editOrNewSpec()
                    .withMinResources(minResources)
                    .endSpec();

            if (this.queue != null) {
                podGroupBuilder
                    .editOrNewSpec()
                    .withQueue(this.queue)
                    .endSpec();
            }
            if (this.minMemberPerJob != null) {
                podGroupBuilder
                        .editOrNewSpec()
                        .withMinMember(Integer.valueOf(this.minMemberPerJob))
                        .endSpec();
            }

            if (this.priorityClassName != null) {
                podGroupBuilder
                        .editOrNewSpec()
                        .withPriorityClassName(this.priorityClassName)
                        .endSpec();
            }

            if (this.queue != null) {
                podGroupBuilder.editOrNewSpec().withQueue(this.queue).endSpec();
            }

            return podGroupBuilder.build();
        }
        return null;
    }

    @Override
    public FlinkPod mergePropertyIntoPod(FlinkPod flinkPod) {
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());
        basicPodBuilder
                .editOrNewSpec()
                .withSchedulerName(this.getClass().getSimpleName().toLowerCase())
                .endSpec();

        if (!this.annotations.isEmpty()) {
            basicPodBuilder.editOrNewMetadata().withAnnotations(this.annotations).endMetadata();
        }
        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }
}
