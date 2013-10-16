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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.ValueSourceScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SubCollector;
import org.apache.solr.search.function.ValueSourceRangeFilter;

import java.io.IOException;
import java.util.Map;

// This class works as either a normal constant score query, or as a PostFilter using a collector
public class FunctionRangeQuery extends SolrConstantScoreQuery implements PostFilter {
  final ValueSourceRangeFilter rangeFilt;

  public FunctionRangeQuery(ValueSourceRangeFilter filter) {
    super(filter);
    this.rangeFilt = filter;
  }

  @Override
  public DelegatingCollector getFilterCollector(IndexSearcher searcher) {
    Map fcontext = ValueSource.newContext(searcher);
    return new FunctionRangeCollector(fcontext);
  }

  class FunctionRangeCollector extends DelegatingCollector {
    final Map fcontext;

    public FunctionRangeCollector(Map fcontext) {
      this.fcontext = fcontext;
    }

    @Override
    public SubCollector subCollector(final AtomicReaderContext context)
        throws IOException {
      final int maxDoc = context.reader().maxDoc();
      final FunctionValues dv = rangeFilt.getValueSource().getValues(fcontext, context);
      final ValueSourceScorer scorer = dv.getRangeScorer(context.reader(),
          rangeFilt.getLowerVal(), rangeFilt.getUpperVal(),
          rangeFilt.isIncludeLower(), rangeFilt.isIncludeUpper()
      );

      final SubCollector delegateSub = delegate.subCollector(context);

      return new SubCollector() {

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          delegateSub.setScorer(scorer);
        }

        @Override
        public void collect(int doc) throws IOException {
          if (doc < maxDoc && scorer.matches(doc)) {
            delegateSub.collect(doc);
          }
        }

        @Override
        public void done() throws IOException {
          delegateSub.done();
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return delegateSub.acceptsDocsOutOfOrder();
        }

      };
    }

  }
}
