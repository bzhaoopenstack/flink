/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.kubeclient.decorators.schedulers;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.parameters.AbstractKubernetesParameters;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.List;
import java.util.Map;

/** TODO. */
public class KubernetesCustomizedScheduler implements CustomizedScheduler {

    protected final Configuration flinkConfig;

    protected final AbstractKubernetesParameters kubernetesComponentConf;

    protected Object targetObject = null;

    public KubernetesCustomizedScheduler(
            AbstractKubernetesParameters kubernetesComponentConf, Configuration flinkConfig) {
        this.kubernetesComponentConf = Preconditions.checkNotNull(kubernetesComponentConf);
        this.flinkConfig = Preconditions.checkNotNull(flinkConfig);
    }

    @Override
    public KubernetesCustomizedScheduler getSchedulerByName(String name) {
        String targetClassPath = getSchedulerFullyQualifiedClassName(name);
        try {
            // TODO mgrigorov This looks bad.
            // Why not just check that the "name" is "volcano" and instantiate the class?
            // If the name is unknown then throw a proper exception.
            Class<?> tarClass = Class.forName(targetClassPath);
            Class[] params = {AbstractKubernetesParameters.class, Configuration.class};
            targetObject =
                    tarClass.getDeclaredConstructor(params)
                            .newInstance(this.kubernetesComponentConf, this.flinkConfig);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return (KubernetesCustomizedScheduler) targetObject;
    }

    @Override
    public CustomizedScheduler settingPropertyIntoScheduler(List<Map<String, String>> mapList) {
        return (CustomizedScheduler) targetObject;
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

        String targetClassPath = getSchedulerFullyQualifiedClassName(name);
        try {
            Class.forName(targetClassPath);
        } catch (ClassNotFoundException e) {
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private static String getSchedulerFullyQualifiedClassName(String name) {
        String tarClassName = name.substring(0, 1).toUpperCase() + name.substring(1).toLowerCase();
        String curPkgPath = KubernetesCustomizedScheduler.class.getPackage().getName();
        return curPkgPath + "." + tarClassName;
    }
}
