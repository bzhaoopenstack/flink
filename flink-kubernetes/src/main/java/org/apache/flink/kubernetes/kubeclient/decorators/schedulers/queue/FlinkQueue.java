package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue;

import io.fabric8.kubernetes.api.model.KubernetesResource;

public class FlinkQueue {
    public String getIdOrName() {
        return idOrName;
    }

    public KubernetesResource getKubeResource() {
        return kubeResource;
    }

    public String idOrName;
    public KubernetesResource kubeResource;

    public FlinkQueue(String queueIdOrName, KubernetesResource kubeResource) {
        this.idOrName = queueIdOrName;
        this.kubeResource = kubeResource;
    }
}
