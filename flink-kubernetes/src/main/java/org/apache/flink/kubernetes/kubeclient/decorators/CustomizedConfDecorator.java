package org.apache.flink.kubernetes.kubeclient.decorators;

import io.fabric8.kubernetes.api.model.HasMetadata;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.CustomizedScheduler;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.KubernetesCustomizedScheduler;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CustomizedConfDecorator extends AbstractKubernetesStepDecorator {

    private final AbstractKubernetesParameters kubernetesComponentConf;
    private final Configuration flinkConfig;
    private final String DEFAULT_SCHEDULER_NAME = "default-scheduler";
    private CustomizedScheduler customizedScheduler = null;

    public CustomizedConfDecorator(
            AbstractKubernetesParameters kubernetesComponentConf) {
        this.kubernetesComponentConf = checkNotNull(kubernetesComponentConf);
        this.flinkConfig = checkNotNull(kubernetesComponentConf.getFlinkConfiguration());
        this.customizedScheduler = checkHasCustomizedScheduler(kubernetesComponentConf);
    }

    private KubernetesCustomizedScheduler checkHasCustomizedScheduler(
            AbstractKubernetesParameters kubernetesComponentConf) {
        String configuredSchdulerName = kubernetesComponentConf.getPodSchedulerName();
        if (configuredSchdulerName == null || configuredSchdulerName.equals(DEFAULT_SCHEDULER_NAME)){
            return null;
        }

        KubernetesCustomizedScheduler kubernetesCustomizedScheduler = new KubernetesCustomizedScheduler(this.kubernetesComponentConf, this.flinkConfig);
        return kubernetesCustomizedScheduler.getSchedulerByName(
                configuredSchdulerName);
    }

    @Override
    public FlinkPod decorateFlinkPod(FlinkPod flinkPod) {
        if (this.customizedScheduler == null){
            return flinkPod;
        }

        List<Map<String, String>> attrs = new ArrayList<>();

        // annotations
        attrs.add(kubernetesComponentConf.getCustomizedAnnotations());
        // customized config
        Map<String, String> configs = kubernetesComponentConf.getPodCustomizedConfig();

        attrs.add(kubernetesComponentConf.getPodCustomizedConfig());

        this.customizedScheduler.settingPropertyIntoScheduler(attrs);
        return this.customizedScheduler.mergePropertyIntoPod(flinkPod);
    }

    @Override
    public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
        HasMetadata hasMetadata = this.customizedScheduler.prepareRequestResources();
        if (hasMetadata != null) {
            return Collections.singletonList(hasMetadata);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<HasMetadata> buildPreAccompanyingKubernetesResources() {
        HasMetadata hasMetadata = this.customizedScheduler.prepareRequestResources();
        if (hasMetadata != null) {
            return Collections.singletonList(hasMetadata);
        } else {
            return Collections.emptyList();
        }
    }
}
