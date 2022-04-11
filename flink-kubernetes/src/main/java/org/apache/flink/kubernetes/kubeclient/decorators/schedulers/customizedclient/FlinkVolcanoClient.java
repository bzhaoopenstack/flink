package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient;

import io.fabric8.kubernetes.client.NamespacedKubernetesClient;

import io.fabric8.volcano.client.VolcanoClient;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;

import java.util.concurrent.ExecutorService;

public class FlinkVolcanoClient extends Fabric8FlinkKubeClient implements CustomizedClient {
    public FlinkVolcanoClient(
            Configuration flinkConfig,
            NamespacedKubernetesClient client,
            ExecutorService executorService) {
        super(flinkConfig, client, executorService);
    }

    @Override
    public <C> C transformToExtendedClient(Class<C> type) {
        return this.internalClient.adapt(type);
    }

    public static VolcanoClient getVolcanoClient(Configuration flinkConfig) {
        FlinkKubeClient kubeClient = FlinkKubeClientFactory
                .getInstance().fromConfiguration(flinkConfig, "client");
        return kubeClient.transformToExtendedClient(VolcanoClient.class);
    }

    @Override
    public void refreshJobAssociatedResources(JobID jobId) {

    }
}
