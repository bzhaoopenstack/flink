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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;

import io.fabric8.kubernetes.api.model.HasMetadata;

import java.util.List;
import java.util.Map;

/** TODO. */
public interface CustomizedScheduler {
    /**
     * Apply transformations to the given FlinkPod in accordance with this feature. This can include
     * adding labels/annotations, mounting volumes, and setting startup command or parameters, etc.
     *
     * @return
     */
    KubernetesCustomizedScheduler getSchedulerByName(String name);

    CustomizedScheduler settingPropertyIntoScheduler(List<Map<String, String>> maplist);

    /**
     * Build the accompanying Kubernetes resources that should be introduced to support this
     * feature. This could only be applicable on the client-side submission process.
     */
    FlinkPod mergePropertyIntoPod(FlinkPod flinkPod);

    // TODO mgrigorov consider returning a List<HasMetadata>
    HasMetadata prepareRequestResources();

    Object getJobId();
}
