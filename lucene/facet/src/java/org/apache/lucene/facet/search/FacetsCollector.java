package org.apache.lucene.facet.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.facet.params.FacetSearchParams;
import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SerialCollector;
import org.apache.lucene.search.SubCollector;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;

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

/**
 * A {@link Collector} which executes faceted search and computes the weight of
 * requested facets. To get the facet results you should call
 * {@link #getFacetResults()}.
 * {@link #create(FacetSearchParams, IndexReader, TaxonomyReader)} returns the
 * most optimized {@link FacetsCollector} for the given parameters.
 * 
 * @lucene.experimental
 */
public abstract class FacetsCollector implements Collector {

  private static final class DocsAndScoresCollector extends FacetsCollector {

    public DocsAndScoresCollector(FacetsAccumulator accumulator) {
      super(accumulator);
    }

    @Override
    protected SubCollector createSubCollector(final AtomicReaderContext context) throws IOException {
      return new SubCollector() {

        final FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
        int totalHits;
        float[] scores = new float[64];

        Scorer scorer;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
          this.scorer = scorer;
        }

        @Override
        public final void collect(int doc) throws IOException {
          bits.set(doc);
          if (totalHits >= scores.length) {
            float[] newScores = new float[ArrayUtil.oversize(totalHits + 1, 4)];
            System.arraycopy(scores, 0, newScores, 0, totalHits);
            scores = newScores;
          }
          scores[totalHits] = scorer.score();
          totalHits++;
        }

        @Override
        public final boolean acceptsDocsOutOfOrder() {
          return false;
        }

        @Override
        public void done() throws IOException {
          matchingDocs.add(new MatchingDocs(context, bits, totalHits, scores));
        }
      };
    }
  }

  private final static class DocsOnlyCollector extends FacetsCollector {

    public DocsOnlyCollector(FacetsAccumulator accumulator) {
      super(accumulator);
    }

    @Override
    protected SubCollector createSubCollector(final AtomicReaderContext context) throws IOException {
      return new SubCollector() {

        final FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
        int totalHits;

        @Override
        public void setScorer(Scorer scorer) throws IOException {
        }

        @Override
        public final void collect(int doc) throws IOException {
          totalHits++;
          bits.set(doc);
        }

        @Override
        public final boolean acceptsDocsOutOfOrder() {
          return true;
        }

        @Override
        public void done() throws IOException {
          matchingDocs.add(new MatchingDocs(context, bits, totalHits, null));
        }

      };
    }

  }
  
  /**
   * Holds the documents that were matched in the {@link AtomicReaderContext}.
   * If scores were required, then {@code scores} is not null.
   */
  public final static class MatchingDocs {
    
    public final AtomicReaderContext context;
    public final FixedBitSet bits;
    public final float[] scores;
    public final int totalHits;
    
    public MatchingDocs(AtomicReaderContext context, FixedBitSet bits, int totalHits, float[] scores) {
      this.context = context;
      this.bits = bits;
      this.scores = scores;
      this.totalHits = totalHits;
    }
  }
  
  /**
   * Creates a {@link FacetsCollector} using the {@link
   * FacetsAccumulator} from {@link FacetsAccumulator#create}.
   */
  public static FacetsCollector create(FacetSearchParams fsp, IndexReader indexReader, TaxonomyReader taxoReader) {
    return create(FacetsAccumulator.create(fsp, indexReader, taxoReader, null));
  }

  /**
   * Creates a {@link FacetsCollector} that satisfies the requirements of the
   * given {@link FacetsAccumulator}.
   */
  public static FacetsCollector create(FacetsAccumulator accumulator) {
    if (accumulator.requiresDocScores()) {
      return new DocsAndScoresCollector(accumulator);
    } else {
      return new DocsOnlyCollector(accumulator);
    }
  }

  private final FacetsAccumulator accumulator;
  private List<FacetResult> cachedResults;

  protected final List<MatchingDocs> matchingDocs = new ArrayList<MatchingDocs>();

  protected FacetsCollector(FacetsAccumulator accumulator) {
    this.accumulator = accumulator;
  }

  /** Performs the actual work of {@link #subCollector(AtomicReaderContext)}. */
  protected abstract SubCollector createSubCollector(AtomicReaderContext context) throws IOException;
  
  /**
   * Returns a {@link FacetResult} per {@link FacetRequest} set in
   * {@link FacetSearchParams}. Note that if a {@link FacetRequest} defines a
   * {@link CategoryPath} which does not exist in the taxonomy, an empty
   * {@link FacetResult} will be returned for it.
   */
  public final List<FacetResult> getFacetResults() throws IOException {
    // LUCENE-4893: if results are not cached, counts are multiplied as many
    // times as this method is called. 
    if (cachedResults == null) {
      cachedResults = accumulator.accumulate(matchingDocs);
    }

    return cachedResults;
  }
  
  /**
   * Returns the documents matched by the query, one {@link MatchingDocs} per
   * visited segment.
   */
  public final List<MatchingDocs> getMatchingDocs() {
    return matchingDocs;
  }
  
  /**
   * Allows to reuse the collector between search requests. This method simply
   * clears all collected documents (and scores) information (as well as cached
   * results), and does not attempt to reuse allocated memory spaces.
   */
  public final void reset() {
    matchingDocs.clear();
    cachedResults = null;
  }

  @Override
  public final SubCollector subCollector(AtomicReaderContext context) throws IOException {
    // clear cachedResults - needed in case someone called getFacetResults()
    // before doing a search and didn't call reset(). Defensive code to prevent
    // traps.
    cachedResults = null;
    return createSubCollector(context);
  }

  @Override
  public void setParallelized() {
    // this collector has the same approach for serial & parallel collection
  }

  @Override
  public boolean isParallelizable() {
    return true;
  }

}
