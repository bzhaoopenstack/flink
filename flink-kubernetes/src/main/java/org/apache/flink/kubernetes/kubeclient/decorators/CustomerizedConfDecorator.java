package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.PodBuilder;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CustomerizedConfDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;
    private final Configuration flinkConfig;
    private final String DEFAULT_SCHEDULER_NAME = "default-scheduler";

    public CustomerizedConfDecorator(
            AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.flinkConfig = checkNotNull(kubernetesComponentConf.getFlinkConfiguration());
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        // Check which one need to be processed, jobmanager or taskmanager via metadata label
        final String componentName = kubernetesComponentConf.getLabels().get(Constants.LABEL_COMPONENT_KEY);

        ConfigOption<String> needProcessConfigOption = null;
        if (componentName.equals(Constants.LABEL_COMPONENT_JOB_MANAGER)) {
            needProcessConfigOption = KubernetesConfigOptions.JOB_MANAGER_POD_SCHEDULER_NAME;
        }
        else if (componentName.equals(Constants.LABEL_COMPONENT_TASK_MANAGER)) {
            needProcessConfigOption = KubernetesConfigOptions.TASK_MANAGER_POD_SCHEDULER_NAME;
        }

        // Overwrite field podS
        final String podSchedulerName =
                KubernetesUtils.resolveUserDefinedValue(
                        flinkConfig,
                        needProcessConfigOption,
                        kubernetesComponentConf.getPodSchedulerName(),
                        KubernetesUtils.getPodSchedulerName(flinkPod),
                        "scheduler name");
        if (podSchedulerName == null || podSchedulerName.equals(DEFAULT_SCHEDULER_NAME)){ return flinkPod; }

        final PodBuilder basicPodBuilder = new PodBuilder(flinkPod.getPodWithoutMainContainer());
        basicPodBuilder
                .editOrNewSpec()
                .withSchedulerName(podSchedulerName)
                .endSpec();

        Map<String, String> annotations = kubernetesComponentConf.getCustomerizedAnnotations();
        basicPodBuilder
                .editOrNewMetadata()
                .withAnnotations(annotations)
                .endMetadata();

        return new FlinkPod.Builder(flinkPod).withPod(basicPodBuilder.build()).build();
    }
}
