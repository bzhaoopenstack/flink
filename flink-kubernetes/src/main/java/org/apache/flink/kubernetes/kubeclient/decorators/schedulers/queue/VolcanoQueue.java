package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue;

import io.fabric8.kubernetes.api.model.KubernetesResource;

import io.fabric8.volcano.client.VolcanoClient;
import io.fabric8.volcano.scheduling.v1beta1.Queue;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient.FlinkVolcanoClient;

public class VolcanoQueue extends FlinkQueue{
    public VolcanoQueue(String queueIdOrName, KubernetesResource kubeResource) {
        super(queueIdOrName, kubeResource);
    }
}
