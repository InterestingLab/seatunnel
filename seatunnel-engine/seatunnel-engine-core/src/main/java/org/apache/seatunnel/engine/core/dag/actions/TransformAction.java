/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.core.dag.actions;

import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import lombok.NonNull;

import java.net.URL;
import java.util.List;

public class TransformAction extends AbstractAction {
    private final SeaTunnelTransform<?> transform;

    public TransformAction(int id,
                           @NonNull String name,
                           @NonNull List<Action> upstreams,
                           @NonNull SeaTunnelTransform<?> transform,
                           @NonNull List<URL> jarUrls) {
        super(id, name, upstreams, jarUrls);
        this.transform = transform;
    }

    public TransformAction(int id,
                           @NonNull String name,
                           @NonNull SeaTunnelTransform<?> transform,
                           @NonNull List<URL> jarUrls) {
        super(id, name, jarUrls);
        this.transform = transform;
    }

    public SeaTunnelTransform<?> getTransform() {
        return transform;
    }
}
