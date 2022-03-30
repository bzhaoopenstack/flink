package org.apache.flink.kubernetes.kubeclient.decorators.schedulers;

import io.fabric8.kubernetes.api.model.HasMetadata;

import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import java.util.List;
import java.util.Map;

public interface CustomizedScheduler {
    /**
     * Apply transformations to the given FlinkPod in accordance with this feature. This can include
     * adding labels/annotations, mounting volumes, and setting startup command or parameters, etc.
     * @return
     */
    KubernetesCustomizedScheduler getSchedulerByName(String name);

    CustomizedScheduler settingPropertyIntoScheduler(List<Map<String, String>> maplist);
    /**
     * Build the accompanying Kubernetes resources that should be introduced to support this
     * feature. This could only be applicable on the client-side submission process.
     */
    FlinkPod mergePropertyIntoPod(FlinkPod flinkPod);

    HasMetadata prepareRequestResources();

    Object getJobId();
}
