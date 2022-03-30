package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.customizedclient;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

public interface CustomizedClient extends FlinkKubeClient {

    <C> C transformToExtendedClient(Class<C> type);
}
