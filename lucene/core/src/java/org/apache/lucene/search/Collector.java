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

import org.apache.lucene.index.AtomicReaderContext;

import java.io.IOException;

/**
 * <p>Expert: Collectors are primarily meant to be used to gather raw results from a search, and implement sorting or
 * custom result filtering, collation, etc. </p>
 * <p>Lucene's core collectors are derived from Collector. Likely your application can use one of these classes, or
 * subclass {@link TopDocsCollector}, instead of implementing Collector directly:
 * <ul>
 * <li>{@link TopDocsCollector} is an abstract base class that assumes you will retrieve the top N docs, according to
 * some criteria, after collection is done.</li>
 * <li>{@link TopScoreDocCollector} is a concrete subclass {@link TopDocsCollector} and sorts according to score +
 * docID.  This is used internally by the {@link IndexSearcher} search methods that do not take an explicit {@link
 * Sort}. It is likely the most frequently used collector.</li>
 * <li>{@link TopFieldCollector} subclasses {@link TopDocsCollector} and sorts according to a specified {@link Sort}
 * object (sort by field).  This is used internally by the {@link IndexSearcher} search methods that take an explicit
 * {@link Sort}.
 * <li>{@link TimeLimitingCollector}, which wraps any other Collector and aborts the search if it's taken too much
 * time.</li>
 * <li>{@link PositiveScoresOnlyCollector} wraps any other Collector and prevents collection of hits whose score is
 * &lt;= 0.0</li>
 * </ul>
 * <p><b>NOTE:</b> Prior to 2.9, Lucene silently filtered out hits with score <= 0.  As of 2.9, the core Collectors no
 * longer do that.  It's very unusual to have such hits (a negative query boost, or function query returning negative
 * custom scores, could cause it to happen).  If you need that behavior, use {@link PositiveScoresOnlyCollector}.</p>
 *
 * @lucene.experimental
 */
public interface Collector {

  /**
   * <p>Called in order to obtain the {@link SubCollector} that will be used for collecting from a {@link
   * AtomicReaderContext}.</p>
   * <p>Add {@link AtomicReaderContext#docBase} to re-base document ID's in {@link SubCollector#collect(int)}.</p>
   *
   * @param context
   *     atomic reader context for this sub-collection
   * @return the SubCollector that will be used for the supplied context
   * @throws IOException
   */
  SubCollector subCollector(AtomicReaderContext context) throws IOException;

  /**
   * <p>Advise this collector about whether parallel collection will be performed.</p>
   * <p>This method can only be called if {@link #isParallelizable()} returns {@code true}, and will only be called
   * before any call to {@link #subCollector(org.apache.lucene.index.AtomicReaderContext)}.<p/>
   * <p>This is intended as a way for implementations to adjust strategy in case the optimal solution is very different
   * for single-threaded vs parallel collection, e.g. it may be desirable to accumulate shared state rather than
   * divide-and-conquer.</p>
   */
  void setParallelized();

  /**
   * Parallel collection implies that besides a primary collection thread which invokes the methods on this
   * {@link Collector} implementation, another thread may be responsible for invocation of all {@link SubCollector}
   * methods except for the {@link SubCollector#done()} method which can be safely assumed to always be invoked from
   * the primary collection thread.
   *
   * @return whether parallel collection is supported
   */
  boolean isParallelizable();

}
