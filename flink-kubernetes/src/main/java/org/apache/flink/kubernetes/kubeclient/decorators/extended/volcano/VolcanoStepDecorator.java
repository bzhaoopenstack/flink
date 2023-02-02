package org.apache.flink.kubernetes.kubeclient.decorators.extended.volcano;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.extended.ExtPluginDecorator;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.volcano.scheduling.v1beta1.PodGroupBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The interface class which should be implemented by plugin decorators. */
public class VolcanoStepDecorator implements ExtPluginDecorator {
    private static final String DEFAULT_SCHEDULER_NAME = "default-scheduler";
    private AbstractKubernetesParameters kubernetesComponentConf = null;
    private Configuration flinkConfig = null;
    private Boolean isTaskManager = Boolean.FALSE;
    private FlinkKubeClient queryClient = null;
    private String priorityClassKey = "priorityclass";
    private List<HasMetadata> myPreCreateResources = null;
    // private KubernetesClient testClient = null;

    public VolcanoStepDecorator() {}

    @Override
    public void configure(AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.flinkConfig = checkNotNull(kubernetesComponentConf.getFlinkConfiguration());
        if (this.kubernetesComponentConf instanceof KubernetesTaskManagerParameters) {
            this.isTaskManager = Boolean.TRUE;
        }
        this.queryClient =
                FlinkKubeClientFactory.getInstance().fromConfiguration(this.flinkConfig, "client");
        // this.testClient = queryClient.getKubernetesClient();
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
            // HasMetadata resourceByType = this.queryClient.getResourceByType(
            //        PodGroup.class, "pg-" + fakename);
            return null;
        }

        basicPodBuilder.editOrNewSpec().withSchedulerName("volcano").endSpec();

        basicPodBuilder
                .editOrNewMetadata()
                .withAnnotations(
                        Collections.singletonMap("scheduling.k8s.io/group-name", "pg-" + fakename))
                .endMetadata();

        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public List<HasMetadata> buildPrePreparedResources() {
        if (!this.isTaskManager) {
            String fakename = this.kubernetesComponentConf.getClusterId();
            PodGroupBuilder podGroupBuilder = new PodGroupBuilder();
            podGroupBuilder.editOrNewMetadata().withName("pg-" + fakename).endMetadata();
            Map<String, String> podGroupConfig = kubernetesComponentConf.getPodGroupConfig();
            for (Map.Entry<String, String> stringStringEntry : podGroupConfig.entrySet()) {
                if (stringStringEntry.getKey().toLowerCase().equals(priorityClassKey)) {
                    podGroupBuilder
                            .editOrNewSpec()
                            .withPriorityClassName(stringStringEntry.getValue())
                            .endSpec();

                    KubernetesJobManagerParameters kubernetesJobManagerParameters =
                            (KubernetesJobManagerParameters) kubernetesComponentConf;
                    double jobManagerCPU = kubernetesJobManagerParameters.getJobManagerCPU();
                    String s = Double.toString(jobManagerCPU);

                    podGroupBuilder
                            .editOrNewSpec()
                            .withMinResources(Collections.singletonMap("cpu", new Quantity(s)))
                            .withMinMember(1)
                            .withQueue("default")
                            .endSpec();
                }
            }

            myPreCreateResources = Collections.singletonList(podGroupBuilder.build());
            return myPreCreateResources;
        }

        return Collections.emptyList();
    }
}
