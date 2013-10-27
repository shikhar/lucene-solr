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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader; // javadocs
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.NIOFSDirectory;    // javadoc
import org.apache.lucene.index.IndexWriter; // javadocs

/** Implements search over a single IndexReader.
 *
 * <p>Applications usually need only call the inherited
 * {@link #search(Query,int)}
 * or {@link #search(Query,Filter,int)} methods. For
 * performance reasons, if your index is unchanging, you
 * should share a single IndexSearcher instance across
 * multiple searches instead of creating a new one
 * per-search.  If your index has changed and you wish to
 * see the changes reflected in searching, you should
 * use {@link DirectoryReader#openIfChanged(DirectoryReader)}
 * to obtain a new reader and
 * then create a new IndexSearcher from that.  Also, for
 * low-latency turnaround it's best to use a near-real-time
 * reader ({@link DirectoryReader#open(IndexWriter,boolean)}).
 * Once you have a new {@link IndexReader}, it's relatively
 * cheap to create a new IndexSearcher from it.
 * 
 * <a name="thread-safety"></a><p><b>NOTE</b>: <code>{@link
 * IndexSearcher}</code> instances are completely
 * thread safe, meaning multiple threads can call any of its
 * methods, concurrently.  If your application requires
 * external synchronization, you should <b>not</b>
 * synchronize on the <code>IndexSearcher</code> instance;
 * use your own (non-Lucene) objects instead.</p>
 */
public class IndexSearcher {
  final IndexReader reader; // package private for testing!
  
  // NOTE: these members might change in incompatible ways
  // in the next release
  protected final IndexReaderContext readerContext;
  protected final List<AtomicReaderContext> leafContexts;

  // This is used for parallel search
  protected final ExecutorService executor;

  // the default Similarity
  private static final Similarity defaultSimilarity = new DefaultSimilarity();
  
  /**
   * Expert: returns a default Similarity instance.
   * In general, this method is only called to initialize searchers and writers.
   * User code and query implementations should respect
   * {@link IndexSearcher#getSimilarity()}.
   * @lucene.internal
   */
  public static Similarity getDefaultSimilarity() {
    return defaultSimilarity;
  }
  
  /** The Similarity implementation used by this searcher. */
  private Similarity similarity = defaultSimilarity;

  /** Creates a searcher searching the provided index. */
  public IndexSearcher(IndexReader r) {
    this(r, null);
  }

  /** Runs searches for each segment separately, using the
   *  provided ExecutorService.  IndexSearcher will not
   *  shutdown/awaitTermination this ExecutorService on
   *  close; you must do so, eventually, on your own.  NOTE:
   *  if you are using {@link NIOFSDirectory}, do not use
   *  the shutdownNow method of ExecutorService as this uses
   *  Thread.interrupt under-the-hood which can silently
   *  close file descriptors (see <a
   *  href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @lucene.experimental */
  public IndexSearcher(IndexReader r, ExecutorService executor) {
    this(r.getContext(), executor);
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   * <p>
   * Given a non-<code>null</code> {@link ExecutorService} this method runs
   * searches for each segment separately, using the provided ExecutorService.
   * IndexSearcher will not shutdown/awaitTermination this ExecutorService on
   * close; you must do so, eventually, on your own. NOTE: if you are using
   * {@link NIOFSDirectory}, do not use the shutdownNow method of
   * ExecutorService as this uses Thread.interrupt under-the-hood which can
   * silently close file descriptors (see <a
   * href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   * 
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context, ExecutorService executor) {
    assert context.isTopLevel: "IndexSearcher's ReaderContext must be topLevel for reader" + context.reader();
    reader = context.reader();
    this.executor = executor;
    this.readerContext = context;
    leafContexts = context.leaves();
  }

  /**
   * Creates a searcher searching the provided top-level {@link IndexReaderContext}.
   *
   * @see IndexReaderContext
   * @see IndexReader#getContext()
   * @lucene.experimental
   */
  public IndexSearcher(IndexReaderContext context) {
    this(context, null);
  }

  /** Return the {@link IndexReader} this searches. */
  public IndexReader getIndexReader() {
    return reader;
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID)</code> 
   * @see IndexReader#document(int) 
   */
  public Document doc(int docID) throws IOException {
    return reader.document(docID);
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID, fieldVisitor)</code>
   * @see IndexReader#document(int, StoredFieldVisitor) 
   */
  public void doc(int docID, StoredFieldVisitor fieldVisitor) throws IOException {
    reader.document(docID, fieldVisitor);
  }

  /** 
   * Sugar for <code>.getIndexReader().document(docID, fieldsToLoad)</code>
   * @see IndexReader#document(int, Set) 
   */
  public Document doc(int docID, Set<String> fieldsToLoad) throws IOException {
    return reader.document(docID, fieldsToLoad);
  }
  
  /**
   * @deprecated Use {@link #doc(int, Set)} instead.
   */
  @Deprecated
  public final Document document(int docID, Set<String> fieldsToLoad) throws IOException {
    return doc(docID, fieldsToLoad);
  }

  /** Expert: Set the Similarity implementation used by this IndexSearcher.
   *
   */
  public void setSimilarity(Similarity similarity) {
    this.similarity = similarity;
  }

  public Similarity getSimilarity() {
    return similarity;
  }
  
  /** @lucene.internal */
  protected Query wrapFilter(Query query, Filter filter) {
    return (filter == null) ? query : new FilteredQuery(query, filter);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous 
   * result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int n) throws IOException {
    return search(createNormalizedWeight(query), after, n);
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null,
   * where all results are after a previous result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n) throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), after, n);
  }
  
  /** Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs search(Query query, int n)
    throws IOException {
    return search(query, null, n);
  }


  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs search(Query query, Filter filter, int n)
    throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), null, n);
  }

  /** Lower-level search API.
   *
   * <p>{@link SubCollector#collect(int)} is called for every matching
   * document.
   *
   * @param query to match documents
   * @param filter if non-null, used to permit documents to be collected.
   * @param results to receive hits
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public void search(Query query, Filter filter, Collector results)
    throws IOException {
    search(leafContexts, createNormalizedWeight(wrapFilter(query, filter)), results);
  }

  /** Lower-level search API.
   *
   * <p>{@link SubCollector#collect(int)} is called for every matching document.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public void search(Query query, Collector results)
    throws IOException {
    search(leafContexts, createNormalizedWeight(query), results);
  }
  
  /** Search implementation with arbitrary sorting.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.
   * 
   * <p>NOTE: this does not compute scores by default; use
   * {@link IndexSearcher#search(Query,Filter,int,Sort,boolean,boolean)} to
   * control scoring.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopFieldDocs search(Query query, Filter filter, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), n, sort, false, false);
  }

  /** Search implementation with arbitrary sorting, plus
   * control over whether hit scores and max score
   * should be computed.  Finds
   * the top <code>n</code> hits for <code>query</code>, applying
   * <code>filter</code> if non-null, and sorting the hits by the criteria in
   * <code>sort</code>.  If <code>doDocScores</code> is <code>true</code>
   * then the score of each hit will be computed and
   * returned.  If <code>doMaxScore</code> is
   * <code>true</code> then the maximum score over all
   * collected hits will be computed.
   * 
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopFieldDocs search(Query query, Filter filter, int n,
                             Sort sort, boolean doDocScores, boolean doMaxScore) throws IOException {
    return search(createNormalizedWeight(wrapFilter(query, filter)), n, sort, doDocScores, doMaxScore);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null,
   * where all results are after a previous result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort) throws IOException {
    if (after != null && !(after instanceof FieldDoc)) {
      // TODO: if we fix type safety of TopFieldDocs we can
      // remove this
      throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
    }
    return search(createNormalizedWeight(wrapFilter(query, filter)), (FieldDoc) after, n, sort, true, false, false);
  }

  /**
   * Search implementation with arbitrary sorting and no filter.
   * @param query The query to search for
   * @param n Return only the top n results
   * @param sort The {@link org.apache.lucene.search.Sort} object
   * @return The top docs, sorted according to the supplied {@link org.apache.lucene.search.Sort} instance
   * @throws IOException if there is a low-level I/O error
   */
  public TopFieldDocs search(Query query, int n,
                             Sort sort) throws IOException {
    return search(createNormalizedWeight(query), n, sort, false, false);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous 
   * result (<code>after</code>).
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, int n, Sort sort) throws IOException {
    if (after != null && !(after instanceof FieldDoc)) {
      // TODO: if we fix type safety of TopFieldDocs we can
      // remove this
      throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
    }
    return search(createNormalizedWeight(query), (FieldDoc) after, n, sort, true, false, false);
  }

  /** Finds the top <code>n</code>
   * hits for <code>query</code> where all results are after a previous 
   * result (<code>after</code>), allowing control over
   * whether hit scores and max score should be computed.
   * <p>
   * By passing the bottom result from a previous page as <code>after</code>,
   * this method can be used for efficient 'deep-paging' across potentially
   * large result sets.  If <code>doDocScores</code> is <code>true</code>
   * then the score of each hit will be computed and
   * returned.  If <code>doMaxScore</code> is
   * <code>true</code> then the maximum score over all
   * collected hits will be computed.
   *
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public TopDocs searchAfter(ScoreDoc after, Query query, Filter filter, int n, Sort sort,
                             boolean doDocScores, boolean doMaxScore) throws IOException {
    if (after != null && !(after instanceof FieldDoc)) {
      // TODO: if we fix type safety of TopFieldDocs we can
      // remove this
      throw new IllegalArgumentException("after must be a FieldDoc; got " + after);
    }
    return search(createNormalizedWeight(wrapFilter(query, filter)), (FieldDoc) after, n, sort, true,
                  doDocScores, doMaxScore);
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>, applying <code>filter</code> if non-null.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  protected TopDocs search(Weight weight, ScoreDoc after, int nDocs) throws IOException {
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    if (after != null && after.doc >= limit) {
      throw new IllegalArgumentException("after.doc exceeds the number of documents in that reader: after.doc="
          + after.doc + " limit=" + limit);
    }
    return search(leafContexts, weight, after, Math.min(nDocs, limit));
  }

  /** Expert: Low-level search implementation.  Finds the top <code>n</code>
   * hits for <code>query</code>.
   *
   * <p>Applications should usually call {@link IndexSearcher#search(Query,int)} or
   * {@link IndexSearcher#search(Query,Filter,int)} instead.
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  protected TopDocs search(List<AtomicReaderContext> leaves, Weight weight, ScoreDoc after, int nDocs) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);
    TopScoreDocCollector collector = TopScoreDocCollector.create(nDocs, after, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, collector);
    return collector.topDocs();
  }

  /** Expert: Low-level search implementation with arbitrary
   * sorting and control over whether hit scores and max
   * score should be computed.  Finds
   * the top <code>n</code> hits for <code>query</code> and sorting the hits
   * by the criteria in <code>sort</code>.
   *
   * <p>Applications should usually call {@link
   * IndexSearcher#search(Query,Filter,int,Sort)} instead.
   * 
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  protected TopFieldDocs search(Weight weight,
                                final int nDocs, Sort sort,
                                boolean doDocScores, boolean doMaxScore) throws IOException {
    return search(weight, null, nDocs, sort, true, doDocScores, doMaxScore);
  }

  /**
   * Just like {@link #search(Weight, int, Sort, boolean, boolean)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   */
  protected TopFieldDocs search(Weight weight, FieldDoc after, int nDocs,
                                Sort sort, boolean fillFields,
                                boolean doDocScores, boolean doMaxScore)
      throws IOException {

    if (sort == null) throw new NullPointerException("Sort must not be null");
    
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    return search(leafContexts, weight, after, Math.min(nDocs, limit), sort, fillFields, doDocScores, doMaxScore);
  }
  
  
  /**
   * Just like {@link #search(Weight, int, Sort, boolean, boolean)}, but you choose
   * whether or not the fields in the returned {@link FieldDoc} instances should
   * be set by specifying fillFields.
   */
  protected TopFieldDocs search(List<AtomicReaderContext> leaves, Weight weight, FieldDoc after, int nDocs,
                                Sort sort, boolean fillFields, boolean doDocScores, boolean doMaxScore) throws IOException {
    // single thread
    int limit = reader.maxDoc();
    if (limit == 0) {
      limit = 1;
    }
    nDocs = Math.min(nDocs, limit);

    TopFieldCollector collector = TopFieldCollector.create(sort, nDocs, after,
                                                           fillFields, doDocScores,
                                                           doMaxScore, !weight.scoresDocsOutOfOrder());
    search(leaves, weight, collector);
    return (TopFieldDocs) collector.topDocs();
  }

  /**
   * Lower-level search API.
   * 
   * <p>
   * {@link SubCollector#collect(int)} is called for every document. <br>
   * 
   * <p>
   * NOTE: this method executes the searches on all given leaves exclusively.
   * To search across all the searchers leaves use {@link #leafContexts}.
   * 
   * @param leaves 
   *          the searchers leaves to execute the searches on
   * @param weight
   *          to match documents
   * @param collector
   *          to receive hits
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  protected void search(List<AtomicReaderContext> leaves, final Weight weight, final Collector collector)
      throws IOException {
    if (executor != null && collector.isParallelizable() && leaves.size() > 1) {
      searchParallel(leaves, weight, collector);
    } else {
      searchSerial(leaves, weight, collector);
    }
  }

  protected void searchSerial(List<AtomicReaderContext> leaves, final Weight weight, final Collector collector)
      throws IOException {
    for (final AtomicReaderContext ctx : leaves) { // search each subreader
      final SubCollector sub;
      try {
        sub = collector.subCollector(ctx);
      } catch (CollectionTerminatedException e) {
        continue; // there is no doc of interest in this reader context
      }
      collect(ctx, weight, sub);
      sub.done();
    }
  }

  protected void searchParallel(List<AtomicReaderContext> leaves, final Weight weight, final Collector collector)
      throws IOException {
    collector.setParallelized();

    final List<Future<SubCollector>> futures = new ArrayList<Future<SubCollector>>(leaves.size());

    for (final AtomicReaderContext ctx : leaves) { // search each subreader
      final SubCollector sub;
      try {
        sub = collector.subCollector(ctx);
      } catch (CollectionTerminatedException e) {
        continue; // there is no doc of interest in this reader context
      }
      futures.add(executor.submit(new Callable<SubCollector>() {
        @Override
        public SubCollector call() throws IOException {
          collect(ctx, weight, sub);
          return sub;
        }
      }));
    }

    for (Future<SubCollector> f: futures) {
      if (f instanceof FutureTask<?>) {
        ((FutureTask<?>) f).run(); // help out if it hasn't begun executing, rather than blocking idly
      }
    }

    for (Future<SubCollector> f: futures) {
      final SubCollector sub;
      try {
        sub = f.get();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        } else {
          throw new RuntimeException(cause);
        }
      }
      sub.done(); // SubCollector.done() is guaranteed to execute in the primary thread
    }
  }

  private static void collect(AtomicReaderContext ctx, Weight weight, SubCollector sub) throws IOException {
    final Scorer scorer = weight.scorer(ctx, !sub.acceptsDocsOutOfOrder(), true, ctx.reader().getLiveDocs());
    if (scorer != null) {
      try {
        scorer.score(sub);
      } catch (CollectionTerminatedException e) {
        // collection was terminated prematurely
      }
    }
  }

  /** Expert: called to re-write queries into primitive queries.
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  public Query rewrite(Query original) throws IOException {
    Query query = original;
    for (Query rewrittenQuery = query.rewrite(reader); rewrittenQuery != query;
         rewrittenQuery = query.rewrite(reader)) {
      query = rewrittenQuery;
    }
    return query;
  }

  /** Returns an Explanation that describes how <code>doc</code> scored against
   * <code>query</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   */
  public Explanation explain(Query query, int doc) throws IOException {
    return explain(createNormalizedWeight(query), doc);
  }

  /** Expert: low-level implementation method
   * Returns an Explanation that describes how <code>doc</code> scored against
   * <code>weight</code>.
   *
   * <p>This is intended to be used in developing Similarity implementations,
   * and, for good performance, should not be displayed with every hit.
   * Computing an explanation is as expensive as executing the query over the
   * entire index.
   * <p>Applications should call {@link IndexSearcher#explain(Query, int)}.
   * @throws BooleanQuery.TooManyClauses If a query would exceed 
   *         {@link BooleanQuery#getMaxClauseCount()} clauses.
   */
  protected Explanation explain(Weight weight, int doc) throws IOException {
    int n = ReaderUtil.subIndex(doc, leafContexts);
    final AtomicReaderContext ctx = leafContexts.get(n);
    int deBasedDoc = doc - ctx.docBase;
    
    return weight.explain(ctx, deBasedDoc);
  }

  /**
   * Creates a normalized weight for a top-level {@link Query}.
   * The query is rewritten by this method and {@link Query#createWeight} called,
   * afterwards the {@link Weight} is normalized. The returned {@code Weight}
   * can then directly be used to get a {@link Scorer}.
   * @lucene.internal
   */
  public Weight createNormalizedWeight(Query query) throws IOException {
    query = rewrite(query);
    Weight weight = query.createWeight(this);
    float v = weight.getValueForNormalization();
    float norm = getSimilarity().queryNorm(v);
    if (Float.isInfinite(norm) || Float.isNaN(norm)) {
      norm = 1.0f;
    }
    weight.normalize(norm, 1.0f);
    return weight;
  }
  
  /**
   * Returns this searchers the top-level {@link IndexReaderContext}.
   * @see IndexReader#getContext()
   */
  /* sugar for #getReader().getTopReaderContext() */
  public IndexReaderContext getTopReaderContext() {
    return readerContext;
  }

  @Override
  public String toString() {
    return "IndexSearcher(" + reader + "; executor=" + executor + ")";
  }
  
  /**
   * Returns {@link TermStatistics} for a term.
   * 
   * This can be overridden for example, to return a term's statistics
   * across a distributed collection.
   * @lucene.experimental
   */
  public TermStatistics termStatistics(Term term, TermContext context) throws IOException {
    return new TermStatistics(term.bytes(), context.docFreq(), context.totalTermFreq());
  }
  
  /**
   * Returns {@link CollectionStatistics} for a field.
   * 
   * This can be overridden for example, to return a field's statistics
   * across a distributed collection.
   * @lucene.experimental
   */
  public CollectionStatistics collectionStatistics(String field) throws IOException {
    final int docCount;
    final long sumTotalTermFreq;
    final long sumDocFreq;

    assert field != null;
    
    Terms terms = MultiFields.getTerms(reader, field);
    if (terms == null) {
      docCount = 0;
      sumTotalTermFreq = 0;
      sumDocFreq = 0;
    } else {
      docCount = terms.getDocCount();
      sumTotalTermFreq = terms.getSumTotalTermFreq();
      sumDocFreq = terms.getSumDocFreq();
    }
    return new CollectionStatistics(field, reader.maxDoc(), docCount, sumTotalTermFreq, sumDocFreq);
  }
}
