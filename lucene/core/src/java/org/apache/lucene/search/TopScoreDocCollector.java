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

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A {@link Collector} implementation that collects the top-scoring hits,
 * returning them as a {@link TopDocs}. This is used by {@link IndexSearcher} to
 * implement {@link TopDocs}-based search. Hits are sorted by score descending
 * and then (when the scores are tied) docID ascending. When you create an
 * instance of this collector you should know in advance whether documents are
 * going to be collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and
 * {@link Float#NEGATIVE_INFINITY} are not valid scores.  This
 * collector will not properly collect hits with such
 * scores.
 */
public abstract class TopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

  // Assumes docs are scored in order.
  private static class InOrderTopScoreDocCollector extends TopScoreDocCollector {
    private InOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }

    @Override
    public TopScoreDocLeafCollector getLeafCollector(LeafReaderContext context) {
      return new TopScoreDocLeafCollector(context) {
        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector cannot handle these scores:
          assert score != Float.NEGATIVE_INFINITY;
          assert !Float.isNaN(score);

          totalHits++;
          if (score <= pqTop.score) {
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          collectedHits++;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return false;
        }
      };
    }
  }

  // Assumes docs are scored in order.
  private static class InOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;

    private InOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }

    @Override
    public TopScoreDocLeafCollector getLeafCollector(LeafReaderContext context) {
      return new TopScoreDocLeafCollector(context) {

        // this is always after.doc - docBase, to save an add when score == after.score
        final int afterDoc = after.doc - docBase;

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector cannot handle these scores:
          assert score != Float.NEGATIVE_INFINITY;
          assert !Float.isNaN(score);

          totalHits++;

          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            return;
          }

          if (score <= pqTop.score) {
            // Since docs are returned in-order (i.e., increasing doc Id), a document
            // with equal score to pqTop.score cannot compete since HitQueue favors
            // documents with lower doc Ids. Therefore reject those docs too.
            return;
          }
          pqTop.doc = doc + docBase;
          pqTop.score = score;
          pqTop = pq.updateTop();
          collectedHits++;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return false;
        }
      };
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
          ? new TopDocs(getTotalHits(), new ScoreDoc[0], Float.NaN)
          : new TopDocs(getTotalHits(), results);
    }
  }

  // Assumes docs are scored out of order.
  private static class OutOfOrderTopScoreDocCollector extends TopScoreDocCollector {
    private OutOfOrderTopScoreDocCollector(int numHits) {
      super(numHits);
    }

    @Override
    public TopScoreDocLeafCollector getLeafCollector(LeafReaderContext context) {
      return new TopScoreDocLeafCollector(context) {
        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector cannot handle NaN
          assert !Float.isNaN(score);

          totalHits++;
          if (score < pqTop.score) {
            // Doesn't compete w/ bottom entry in queue
            return;
          }
          doc += docBase;
          if (score == pqTop.score && doc > pqTop.doc) {
            // Break tie in score by doc ID:
            return;
          }
          pqTop.doc = doc;
          pqTop.score = score;
          pqTop = pq.updateTop();
          collectedHits++;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
      };
    }
  }

  // Assumes docs are scored out of order.
  private static class OutOfOrderPagingScoreDocCollector extends TopScoreDocCollector {
    private final ScoreDoc after;

    private OutOfOrderPagingScoreDocCollector(ScoreDoc after, int numHits) {
      super(numHits);
      this.after = after;
    }

    @Override
    public TopScoreDocLeafCollector getLeafCollector(LeafReaderContext context) {
      return new TopScoreDocLeafCollector(context) {

        // this is always after.doc - docBase, to save an add when score == after.score
        final int afterDoc = after.doc - docBase;

        @Override
        public void collect(int doc) throws IOException {
          float score = scorer.score();

          // This collector cannot handle NaN
          assert !Float.isNaN(score);

          totalHits++;
          if (score > after.score || (score == after.score && doc <= afterDoc)) {
            // hit was collected on a previous page
            return;
          }
          if (score < pqTop.score) {
            // Doesn't compete w/ bottom entry in queue
            return;
          }
          doc += docBase;
          if (score == pqTop.score && doc > pqTop.doc) {
            // Break tie in score by doc ID:
            return;
          }
          pqTop.doc = doc;
          pqTop.score = score;
          pqTop = pq.updateTop();
          collectedHits++;
        }

        @Override
        public boolean acceptsDocsOutOfOrder() {
          return true;
        }
      };
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
      return results == null
          ? new TopDocs(getTotalHits(), new ScoreDoc[0], Float.NaN)
          : new TopDocs(getTotalHits(), results);
    }
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect and whether documents are scored in order by the input
   * {@link Scorer} to {@link LeafCollector#setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, boolean docsScoredInOrder) {
    return create(numHits, null, docsScoredInOrder);
  }

  /**
   * Creates a new {@link TopScoreDocCollector} given the number of hits to
   * collect, the bottom of the previous page, and whether documents are scored in order by the input
   * {@link Scorer} to {@link LeafCollector#setScorer(Scorer)}.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>, and fill the array with sentinel
   * objects.
   */
  public static TopScoreDocCollector create(int numHits, ScoreDoc after, boolean docsScoredInOrder) {

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    if (docsScoredInOrder) {
      return after == null
          ? new InOrderTopScoreDocCollector(numHits)
          : new InOrderPagingScoreDocCollector(after, numHits);
    } else {
      return after == null
          ? new OutOfOrderTopScoreDocCollector(numHits)
          : new OutOfOrderPagingScoreDocCollector(after, numHits);
    }

  }

  private abstract class TopScoreDocLeafCollector implements LeafCollector {

    final int docBase;

    final CollectionState collectionState;

    final HitQueue pq;
    ScoreDoc pqTop;

    Scorer scorer;

    int totalHits;
    int collectedHits;

    private TopScoreDocLeafCollector(LeafReaderContext context) {
      docBase = context.docBase;

      if (statePool != null) {
        final SortedMap<Integer, CollectionState> candidates =
            acceptsDocsOutOfOrder() ? statePool : statePool.headMap(docBase);
        if (candidates.isEmpty()) {
          collectionState = new CollectionState(new HitQueue(numHits, true));
        } else {
          collectionState = candidates.remove(candidates.lastKey());
        }
      } else {
        collectionState = primordialState;
      }

      pq = collectionState.pq;

      // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
      // that at this point top() is already initialized.
      pqTop = pq.top();
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
    }

    @Override
    public void leafDone() throws IOException {
      collectionState.totalHits += totalHits;
      collectionState.collectedHits += collectedHits;
      if (statePool != null) {
        statePool.put(docBase, collectionState);
      }
    }

  }

  protected static final class CollectionState {
    private final HitQueue pq;
    private int totalHits = 0;
    private int collectedHits = 0;

    CollectionState(HitQueue pq) {
      this.pq = pq;
    }

    void mergeIn(CollectionState other) {
      for (ScoreDoc scoreDoc: other.pq) {
        pq.insertWithOverflow(scoreDoc);
      }
      totalHits += other.totalHits;
      collectedHits += other.collectedHits;
    }
  }

  protected final CollectionState primordialState;
  protected final int numHits;

  protected SortedMap<Integer, CollectionState> statePool;

  protected TopScoreDocCollector(int numHits) {
    super(new HitQueue(numHits, true));
    this.primordialState = new CollectionState((HitQueue) pq);
    this.numHits = numHits;
  }

  public abstract TopScoreDocLeafCollector getLeafCollector(LeafReaderContext context);

  @Override
  public void setParallelized() {
    statePool = new TreeMap<>();
    statePool.put(-1, primordialState);
  }

  @Override
  public void done() {
    if (statePool != null) {
      final Iterator<Map.Entry<Integer, CollectionState>> it = statePool.entrySet().iterator();
      while (it.hasNext()) {
        final CollectionState state = it.next().getValue();
        if (state != primordialState) {
          primordialState.mergeIn(state);
          it.remove();
        }
      }
    }
  }

  @Override
  public boolean isParallelizable() {
    return true;
  }

  @Override
  public int getTotalHits() {
    return primordialState.totalHits;
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      return EMPTY_TOPDOCS;
    }

    // We need to compute maxScore in order to set it in TopDocs. If start == 0,
    // it means the largest element is already in results, use its score as
    // maxScore. Otherwise pop everything else, until the largest element is
    // extracted and use its score as maxScore.
    float maxScore = Float.NaN;
    if (start == 0) {
      maxScore = results[0].score;
    } else {
      for (int i = pq.size(); i > 1; i--) { pq.pop(); }
      maxScore = pq.pop().score;
    }

    return new TopDocs(primordialState.totalHits, results, maxScore);
  }

  @Override
  protected int topDocsSize() {
    return Math.min(primordialState.collectedHits, pq.size());
  }

}