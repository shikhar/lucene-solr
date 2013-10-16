/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search;


import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.SubCollector;

import java.io.IOException;

/** A simple delegating collector where one can set the delegate after creation */
public abstract class DelegatingCollector implements Collector {

  /* for internal testing purposes only to determine the number of times a delegating collector chain was used */
  public static int setLastDelegateCount;

  protected Collector delegate;

  public Collector getDelegate() {
    return delegate;
  }

  public void setDelegate(Collector delegate) {
    this.delegate = delegate;
  }

  /** Sets the last delegate in a chain of DelegatingCollectors */
  public void setLastDelegate(Collector delegate) {
    DelegatingCollector ptr = this;
    for(; ptr.getDelegate() instanceof DelegatingCollector; ptr = (DelegatingCollector)ptr.getDelegate());
    ptr.setDelegate(delegate);
    setLastDelegateCount++;
  }

  @Override
  public void setParallelized() {
    delegate.setParallelized();
  }

  @Override
  public boolean isParallelizable() {
    return delegate.isParallelizable();
  }

  public void finish() throws IOException {
    if (delegate instanceof DelegatingCollector) {
      ((DelegatingCollector) delegate).finish();
    }
  }

}

