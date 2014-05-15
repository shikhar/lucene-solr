package org.apache.solr.search;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

public class TopScoreCollector implements Collector {

  private float topScore = Float.NEGATIVE_INFINITY;

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return new LeafCollector() {

      float topScore = Float.NEGATIVE_INFINITY;

      Scorer scorer;

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        if (score > topScore) topScore = score;
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }

      @Override
      public void leafDone() throws IOException {
        if (topScore > TopScoreCollector.this.topScore) {
          TopScoreCollector.this.topScore = topScore;
        }
      }
    };
  }

  @Override
  public void done() throws IOException {
  }

  @Override
  public boolean isParallelizable() {
    return true;
  }

  @Override
  public void setParallelized() {
  }

  public float getTopScore() {
    return topScore;
  }

}
