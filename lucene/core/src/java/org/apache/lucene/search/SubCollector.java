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

/**
 * <p>A SubCollector is the target of successive calls to {@link #collect(int)} for each document that matched a
 * query.</p>
 * <p>SubCollector decouples the score from the collected doc: the score computation can be skipped entirely if it's
 * not needed. SubCollectors that do need the score should implement the {@link #setScorer(Scorer)} method.</p>
 * <p>It is strongly recommended that {@link Collector#isParallelizable() parallelizable implementations} not achieve
 * the parallelizability by way of blocking constructs like synchronization.</p>
 */
public interface SubCollector {

  /**
   * <p>Called before successive calls to {@link #collect(int)}. Implementations that need the score of the current
   * document (passed-in to {@link #collect(int)}), should save the passed-in {@link Scorer} and call {@code
   * scorer.score()} when needed.</p>
   * <p>If the score for a hit may be requested multiple times, you should use {@link ScoreCachingWrappingScorer}.</p>
   *
   * @param scorer
   */
  void setScorer(Scorer scorer) throws IOException;

  /**
   * <p>Called once for every document matching a query, with the unbased document number.</p>
   * <p>Note: The collection of the current segment can be terminated by throwing a {@link
   * CollectionTerminatedException}. In this case, the last docs of the current {@link
   * org.apache.lucene.index.AtomicReader reader} will be skipped and {@link IndexSearcher} will swallow the exception
   * and continue collection with the next leaf. However {@link #done()} will still be invoked.</p>
   * <p>Note: This is called in an inner search loop. For good search performance, implementations of this method
   * should not call {@link IndexSearcher#doc(int)} or {@link org.apache.lucene.index.IndexReader#document(int)} on
   * every hit. Doing so can slow searches by an order of magnitude or more.</p>
   *
   * @param doc
   *     unbased document ID
   */
  void collect(int doc) throws IOException;

  /**
   * Advise that no further calls to this sub-collector will be made.
   *
   * @throws IOException
   */
  void done() throws IOException;

  /**
   * <p>Return {@code true} if this collector does not require the matching docIDs to be delivered in int sort
   * order (smallest to largest) to {@link #collect}.</p>
   * <p>Most Lucene Query implementations will visit matching docIDs in order. However, some queries (currently limited
   * to certain cases of {@link BooleanQuery}) can achieve faster searching if the {@code SubCollector} allows them to
   * deliver the docIDs out of order.</p>
   * <p>Many collectors don't mind getting docIDs out of order, so it's important to return {@code true}  here.
   *
   * @return whether calls to {@link #collect(int)} must be in int sort order of docID.
   */
  boolean acceptsDocsOutOfOrder();

}
