package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue;

import io.fabric8.volcano.client.VolcanoClient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient.FlinkVolcanoClient;

public class VolcanoQueueFactory extends FlinkKubernetesQueueFactory {
    private static final VolcanoQueueFactory INSTANCE = new VolcanoQueueFactory();
    private VolcanoClient volcanoClient;

    public void InitVolcanoQueueFactory(Configuration flinkConfig) {
        this.volcanoClient = FlinkVolcanoClient.getVolcanoClient(flinkConfig);
    }

    @Override
    public FlinkQueue getQueueByNameOrId(String q) {
        return new VolcanoQueue(q, this.volcanoClient.queues().withName(q).get());
    }

    public static VolcanoQueueFactory getInstance() {
        return INSTANCE;
    }
}
