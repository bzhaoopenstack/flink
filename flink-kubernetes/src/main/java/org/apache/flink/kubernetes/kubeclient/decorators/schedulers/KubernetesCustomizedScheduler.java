package org.apache.flink.kubernetes.kubeclient.decorators.schedulers;

import io.fabric8.kubernetes.api.model.HasMetadata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;

public class KubernetesCustomizedScheduler implements CustomizedScheduler {
    protected Object tarObj = null;
    private Tuple2<String, String> tuple2;
    protected Configuration flinkConfig;
    protected AbstractKubernetesParameters kubernetesComponentConf;

    public KubernetesCustomizedScheduler(AbstractKubernetesParameters kubernetesComponentConf, Configuration flinkConfig) {
        this.kubernetesComponentConf = kubernetesComponentConf;
        this.flinkConfig = flinkConfig;
    }

    @Override
    public KubernetesCustomizedScheduler getSchedulerByName(
            String name) {
        String tarClassName =
                name.substring(0, 1).toUpperCase() + name.substring(1, name.length()).toLowerCase();
        String curPkgPath = KubernetesCustomizedScheduler.class.getPackage().getName();
        String targetClassPath = curPkgPath + "." + tarClassName;
        try {
            Class<?> tarClass = Class.forName(targetClassPath);
            Class[] params = {AbstractKubernetesParameters.class, Configuration.class};
            tarObj = tarClass.getDeclaredConstructor(params).newInstance(this.kubernetesComponentConf, this.flinkConfig);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return (KubernetesCustomizedScheduler) tarObj;
    }

    @Override
    public CustomizedScheduler settingPropertyIntoScheduler(
            List<Map<String, String>> mapList) {
        return (CustomizedScheduler) tarObj;
    }

    @Override
    public FlinkPod mergePropertyIntoPod(FlinkPod flinkPod) {
        return flinkPod;
    }

    @Override
    public HasMetadata prepareRequestResources() {
        return null;
    }

    @Override
    public Object getJobId() {
        return null;
    }

    public static Boolean isSupportCustomizedScheduler(String name) {

        String tarClassName =
                name.substring(0, 1).toUpperCase() + name.substring(1, name.length()).toLowerCase();
        String curPkgPath = KubernetesCustomizedScheduler.class.getPackage().getName();
        String targetClassPath = curPkgPath + "." + tarClassName;
        try {
            Class.forName(targetClassPath);
        } catch (ClassNotFoundException e) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }
}
