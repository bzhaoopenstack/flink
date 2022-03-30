package org.apache.flink.kubernetes.kubeclient.decorators.schedulers.queue;


import io.fabric8.kubernetes.api.model.KubernetesResource;

import java.util.HashMap;



public class FlinkKubernetesQueueFactory implements KubernetesQueueFactory {
    protected HashMap<String, String> resolvedResourcesMap = new HashMap<String, String>();


    @Override
    public FlinkQueue getQueueByNameOrId(String q) {
        return null;
    }

}
