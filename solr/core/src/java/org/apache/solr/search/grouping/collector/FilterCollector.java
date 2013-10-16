package org.apache.solr.search.grouping.collector;

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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.WrappingCollector;
import org.apache.solr.search.DocSet;

import java.io.IOException;

/**
 * A collector that filters incoming doc ids that are not in the filter.
 *
 * @lucene.experimental
 */
public class FilterCollector extends WrappingCollector {

  private final DocSet filter;

  private int matches;

  public FilterCollector(DocSet filter, Collector delegate) {
    super(delegate);
    this.filter = filter;
  }

  public int getMatches() {
    return matches;
  }

  /**
   * Returns the delegate collector
   *
   * @return the delegate collector
   */
  public Collector getDelegate() {
    return delegate;
  }

  @Override
  public WrappingSubCollector subCollector(AtomicReaderContext context) throws IOException {
    final int docBase = context.docBase;
    return new WrappingSubCollector(delegate.subCollector(context)) {

      int subMatches = 0;

      @Override
      public void collect(int doc) throws IOException {
        subMatches++;
        if (filter.exists(doc + docBase)) {
          delegate.collect(doc);
        }
      }

      @Override
      public void done() throws IOException {
        delegate.done();
        FilterCollector.this.matches += subMatches;
      }
    };
  }

}
