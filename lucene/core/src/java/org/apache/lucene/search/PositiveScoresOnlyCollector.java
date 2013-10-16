package org.apache.lucene.search;

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

import java.io.IOException;

import org.apache.lucene.index.AtomicReaderContext;

/**
 * A {@link Collector} implementation which wraps another
 * {@link Collector} and makes sure only documents with
 * scores &gt; 0 are collected.
 */
public class PositiveScoresOnlyCollector extends WrappingCollector {

  public PositiveScoresOnlyCollector(Collector c) {
    super(c);
  }

  @Override
  public WrappingSubCollector subCollector(AtomicReaderContext context) throws IOException {
    return new WrappingSubCollector(delegate.subCollector(context)) {
      Scorer scorer;

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        // Set a ScoreCachingWrappingScorer in case the wrapped Collector will call score() also.
        delegate.setScorer(this.scorer = new ScoreCachingWrappingScorer(scorer));
      }

      @Override
      public void collect(int doc) throws IOException {
        if (scorer.score() > 0) {
          delegate.collect(doc);
        }
      }

    };
  }

}
