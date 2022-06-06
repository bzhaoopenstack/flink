package org.apache.flink.kubernetes.kubeclient.decorators.extended;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;

import io.fabric8.kubernetes.api.model.Quantity;
// profile enabled libraries
import io.fabric8.volcano.scheduling.v1beta1.PodGroup;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.AbstractKubernetesStepDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class VolcanoStepDecorator extends AbstractKubernetesStepDecorator {
    private static final String DEFAULT_SCHEDULER_NAME = "default-scheduler";
    private final AbstractKubernetesParameters kubernetesComponentConf;
    private final Configuration flinkConfig;
    private Boolean isTaskManager = Boolean.FALSE;
    private FlinkKubeClient queryClient;
    private String priorityClassKey = "priorityclass";

    public VolcanoStepDecorator(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.flinkConfig = checkNotNull(kubernetesComponentConf.getFlinkConfiguration());
        if (this.kubernetesComponentConf instanceof KubernetesTaskManagerParameters) {
            this.isTaskManager = Boolean.TRUE;
        }
        this.queryClient = FlinkKubeClientFactory.getInstance()
                .fromConfiguration(this.flinkConfig, "client");
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        String configuredSchedulerName = kubernetesComponentConf.getPodSchedulerName();
        if (configuredSchedulerName == null
                || configuredSchedulerName.equals(DEFAULT_SCHEDULER_NAME)) {
            return flinkPod;
        }
        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());
        String fakename = this.kubernetesComponentConf.getClusterId();
        if (this.isTaskManager) {
            // Need raise an error if PodGroup doesn't exist
            HasMetadata resourceByType = this.queryClient.getResourceByType(
                    PodGroup.class, "pg-" + fakename);
        }

        basicPodBuilder
                .editOrNewSpec()
                .withSchedulerName("volcano")
                .endSpec();

        basicPodBuilder.editOrNewMetadata()
                .withAnnotations(Collections.singletonMap(
                        "scheduling.k8s.io/group-name",
                        "pg-" + fakename))
                .endMetadata();

        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }

    @Override
    public List<HasMetadata> buildPreAccompanyingKubernetesResources() {
        if (!this.isTaskManager) {
            String fakename = this.kubernetesComponentConf.getClusterId();
            PodGroupBuilder podGroupBuilder = new PodGroupBuilder();
            podGroupBuilder
                    .editOrNewMetadata()
                    .withName("pg-" + fakename)
                    .endMetadata();
            Map<String, String> podGroupConfig = kubernetesComponentConf.getPodGroupConfig();
            for (Map.Entry<String, String> stringStringEntry : podGroupConfig.entrySet()) {
                if (stringStringEntry.getKey().toLowerCase().equals(priorityClassKey)) {
                    podGroupBuilder
                            .editOrNewSpec()
                            .withPriorityClassName(stringStringEntry.getValue())
                            .endSpec();

                    KubernetesJobManagerParameters kubernetesJobManagerParameters = (KubernetesJobManagerParameters) kubernetesComponentConf;
                    double jobManagerCPU = kubernetesJobManagerParameters.getJobManagerCPU();
                    String s = Double.toString(jobManagerCPU);

                    podGroupBuilder.editOrNewSpec().withMinResources(Collections.singletonMap("cpu", new Quantity(s)))
                            .withMinMember(1)
                            .withQueue("default")
                            .endSpec();
                }
            }
            return Collections.singletonList(podGroupBuilder.build());
        }
        return Collections.emptyList();
    }
}
