package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue;

public interface KubernetesQueueFactory {
    FlinkQueue getQueueByNameOrId(String q);
}
