package org.apache.lucene.index.sorter;

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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TotalHitCountCollector;

/**
 * A {@link Collector} that early terminates collection of documents on a
 * per-segment basis, if the segment was sorted according to the given
 * {@link Sort}.
 *
 * <p>
 * <b>NOTE:</b> the {@code Collector} detects sorted segments according to
 * {@link SortingMergePolicy}, so it's best used in conjunction with it. Also,
 * it collects up to a specified {@code numDocsToCollect} from each segment,
 * and therefore is mostly suitable for use in conjunction with collectors such as
 * {@link TopDocsCollector}, and not e.g. {@link TotalHitCountCollector}.
 * <p>
 * <b>NOTE</b>: If you wrap a {@code TopDocsCollector} that sorts in the same
 * order as the index order, the returned {@link TopDocsCollector#topDocs() TopDocs}
 * will be correct. However the total of {@link TopDocsCollector#getTotalHits()
 * hit count} will be underestimated since not all matching documents will have
 * been collected.
 * <p>
 * <b>NOTE</b>: This {@code Collector} uses {@link Sort#toString()} to detect
 * whether a segment was sorted with the same {@code Sort}. This has
 * two implications:
 * <ul>
 * <li>if a custom comparator is not implemented correctly and returns
 * different identifiers for equivalent instances, this collector will not
 * detect sorted segments,</li>
 * <li>if you suddenly change the {@link IndexWriter}'s
 * {@code SortingMergePolicy} to sort according to another criterion and if both
 * the old and the new {@code Sort}s have the same identifier, this
 * {@code Collector} will incorrectly detect sorted segments.</li>
 * </ul>
 *
 * @lucene.experimental
 */
public class EarlyTerminatingSortingCollector extends FilterCollector {

  /** Sort used to sort the search results */
  protected final Sort sort;
  /** Number of documents to collect in each segment */
  protected final int numDocsToCollect;

  /**
   * Create a new {@link EarlyTerminatingSortingCollector} instance.
   *
   * @param in
   *          the collector to wrap
   * @param sort
   *          the sort you are sorting the search results on
   * @param numDocsToCollect
   *          the number of documents to collect on each segment. When wrapping
   *          a {@link TopDocsCollector}, this number should be the number of
   *          hits.
   */
  public EarlyTerminatingSortingCollector(Collector in, Sort sort, int numDocsToCollect) {
    super(in);
    if (numDocsToCollect <= 0) {
      throw new IllegalStateException("numDocsToCollect must always be > 0, got " + numDocsToCollect);
    }
    this.sort = sort;
    this.numDocsToCollect = numDocsToCollect;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    if (SortingMergePolicy.isSorted(context.reader(), sort)) {
      // segment is sorted, can early-terminate
      return new FilterLeafCollector(super.getLeafCollector(context)) {
        private int numCollected;

        @Override
        public void collect(int doc) throws IOException {
          super.collect(doc);
          if (++numCollected >= numDocsToCollect) {
            throw new CollectionTerminatedException();
          }
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return false;
        }

      };
    } else {
      return super.getLeafCollector(context);
    }
  }

  @Override
  public boolean isParallelizable() {
    return in.isParallelizable();
  }

}
