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

package org.apache.seatunnel.api.tracing;

import java.util.Comparator;
import java.util.function.Supplier;

public class MDCComparator<T> implements Comparator<T> {
    private final Supplier<MDCContext> contextSupplier;
    private final Comparator<T> delegate;

    public MDCComparator(Comparator<T> delegate) {
        this(MDCContext.current(), delegate);
    }

    public MDCComparator(MDCContext context, Comparator<T> delegate) {
        this(() -> context, delegate);
    }

    public MDCComparator(Supplier<MDCContext> contextSupplier, Comparator<T> delegate) {
        this.contextSupplier = contextSupplier;
        this.delegate = delegate;
    }

    @Override
    public int compare(T o1, T o2) {
        try (MDCContext ignored = contextSupplier.get().activate()) {
            return delegate.compare(o1, o2);
        }
    }
}
