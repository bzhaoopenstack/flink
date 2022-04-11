package org.apache.flink.kubernetes.kubeclient.decorators.schedulers;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResource;
import io.fabric8.kubernetes.api.model.PodBuilder;

import io.fabric8.volcano.client.VolcanoClient;

import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder;
import io.fabric8.kubernetes.api.model.Quantity;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient.FlinkVolcanoClient;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue.FlinkQueue;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue.VolcanoQueueFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static sun.tools.java.Constants.NULL;

public class Volcano extends KubernetesCustomizedScheduler {
    @Override
    public Object getJobId() {
        return jobId;
    }

    private Object jobId = null;
    private String queue;
    private String min_member_per_job;
    private String minMemberKey = "minmember";
    private String min_cpu_per_job;
    private String minCpuKey = "mincpu";

    private String min_memory_per_job;
    private String minMemoryKey = "minmemory";
    private String priorityClassName;
    private String priorityClassKey = "priorityclass";
    private Map<String, String> annotations;
    private String jobPrefix = "pod-group-";
    private final String QUEUE_PREFIX = "scheduling.volcano.sh/queue-name";
    private final String PODGROUP_PREFIX = "scheduling.k8s.io/group-name";
    private VolcanoClient volcanoClient;


    public Volcano(
            AbstractKubernetesParameters kubernetesComponentConf,
            Configuration flinkConfig) {
        super(kubernetesComponentConf, flinkConfig);
        this.volcanoClient = FlinkVolcanoClient.getVolcanoClient(this.flinkConfig);
        if (this.flinkConfig
                .get(DeploymentOptions.TARGET)
                .equals(KubernetesDeploymentTarget.APPLICATION.getName())) {
            this.jobId = this.flinkConfig
                    .getOptional(KubernetesConfigOptions.CLUSTER_ID);
        }
        else if (this.flinkConfig
                .getString(DeploymentOptions.TARGET)
                .equals(KubernetesDeploymentTarget.SESSION
                        .getName())) {
            if (kubernetesComponentConf.getAssociatedJobs() != null && kubernetesComponentConf.getAssociatedJobs().size() >= 1) {
                this.jobId = kubernetesComponentConf.getAssociatedJobs().toArray()[0];
            }

        }
    }

    @Override
    public CustomizedScheduler settingPropertyIntoScheduler(
            List<Map<String, String>> mapList) {

        this.annotations = mapList.get(0);
        this.queue = this.annotations.getOrDefault(QUEUE_PREFIX, null);
        if (!this.annotations.containsKey(PODGROUP_PREFIX) && this.jobId != null) {
            if (this.annotations.isEmpty()) {
                this.annotations = Collections.singletonMap(PODGROUP_PREFIX, this.jobPrefix + this.jobId.toString());
            } else {
                this.annotations.put(PODGROUP_PREFIX, this.jobPrefix + this.jobId.toString());
            }
        }

        Map<String, String> configs = mapList.get(1);

        for (Map.Entry<String, String> stringStringEntry : configs.entrySet()) {
            if (stringStringEntry.getKey().toLowerCase().equals(minMemberKey)) {
                this.min_member_per_job = stringStringEntry.getValue();
            } else if (stringStringEntry.getKey().toLowerCase().equals(minCpuKey)) {
                this.min_cpu_per_job = stringStringEntry.getValue();
            }
            else if (stringStringEntry.getKey().toLowerCase().equals(minMemoryKey)) {
                this.min_memory_per_job = stringStringEntry.getValue();
            }
            else if (stringStringEntry.getKey().toLowerCase().equals(priorityClassKey)) {
                this.priorityClassName = stringStringEntry.getValue();
            }
        }

        return this;
    }

    @Override
    public HasMetadata prepareRequestResources() {
        if (this.queue != null) {
            VolcanoQueueFactory.getInstance().InitVolcanoQueueFactory(this.flinkConfig);
            FlinkQueue queue = VolcanoQueueFactory
                    .getInstance()
                    .getQueueByNameOrId(this.queue);
            KubernetesResource kubeResource = queue.getKubeResource();
        }

        HashMap<String, Quantity> minResources = new HashMap<>();
        if (this.min_cpu_per_job != null) {
            minResources.put(
                    Constants.RESOURCE_NAME_CPU,
                    new Quantity(this.min_cpu_per_job));
        }
        if (this.min_memory_per_job != null) {
            minResources.put(Constants.RESOURCE_NAME_MEMORY, new Quantity(this.min_memory_per_job, Constants.RESOURCE_UNIT_MB));
        }

        if (this.jobId != null) {
            String namespace = this.volcanoClient.getNamespace();
            PodGroupBuilder podGroupBuilder = new PodGroupBuilder();
            podGroupBuilder
                    .editOrNewMetadata()
                    .withName(this.jobPrefix + this.jobId.toString())
                    .withNamespace(namespace)
                    .endMetadata()
                    .editOrNewSpec()
                    .withMinResources(minResources)
                    .endSpec();

            if (this.min_member_per_job != null) {
                podGroupBuilder
                        .editOrNewSpec()
                        .withMinMember(Integer.valueOf(this.min_member_per_job))
                        .endSpec();
            }

            if (this.priorityClassName != null) {
                podGroupBuilder
                        .editOrNewSpec()
                        .withPriorityClassName(this.priorityClassName)
                        .endSpec();
            }

            if (this.queue != null) {
                podGroupBuilder
                        .editOrNewSpec()
                        .withQueue(this.queue)
                        .endSpec();
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
            basicPodBuilder
                    .editOrNewMetadata()
                    .withAnnotations(this.annotations)
                    .endMetadata();
        }
        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }
}
