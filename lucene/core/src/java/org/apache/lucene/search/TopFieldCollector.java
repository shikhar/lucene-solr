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
import org.apache.lucene.search.FieldValueHitQueue.Entry;

/**
 * A {@link Collector} that sorts by {@link SortField} using
 * {@link FieldComparator}s.
 * <p/>
 * See the {@link #create(org.apache.lucene.search.Sort, int, boolean, boolean, boolean, boolean)} method
 * for instantiating a TopFieldCollector.
 * 
 * @lucene.experimental
 */
public abstract class TopFieldCollector extends TopDocsCollector<Entry> {

  // TODO: one optimization we could do is to pre-fill
  // the queue with sentinel value that guaranteed to
  // always compare lower than a real hit; this would
  // save having to check queueFull on each insert

  /*
   * Implements a TopFieldCollector over one SortField criteria, without
   * tracking document scores and maxScore.
   */
  private class OneComparatorNonScoringSubCollector extends TopFieldSubCollector {

    final FieldComparator<?> comparator;
    final int reverseMul;

    OneComparatorNonScoringSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
      comparator = queue.firstComparator;
      reverseMul = queue.reverseMul[0];
    }

    final void updateBottom(int doc) {
      // bottom.score is already set to Float.NaN in add().
      bottom.doc = docBase + doc;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is larger than anything else in the queue, and
          // therefore not competitive.
          return;
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, Float.NaN);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      comparator.setScorer(scorer);
    }

  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, without
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private class OutOfOrderOneComparatorNonScoringSubCollector extends OneComparatorNonScoringSubCollector {

    public OutOfOrderOneComparatorNonScoringSubCollector(AtomicReaderContext context)
        throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, Float.NaN);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, while tracking
   * document scores but no maxScore.
   */
  private class OneComparatorScoringNoMaxScoreSubCollector extends OneComparatorNonScoringSubCollector {

    Scorer scorer;

    public OneComparatorScoringNoMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is largest than anything else in the queue, and
          // therefore not competitive.
          return;
        }
        
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      comparator.setScorer(scorer);
    }
    
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, while tracking
   * document scores but no maxScore, and assumes out of orderness in doc Ids
   * collection.
   */
  private class OutOfOrderOneComparatorScoringNoMaxScoreSubCollector extends OneComparatorScoringNoMaxScoreSubCollector {

    public OutOfOrderOneComparatorScoringNoMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Compute the score only if the hit is competitive.
        final float score = scorer.score();

        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore.
   */
  private class OneComparatorScoringMaxScoreSubCollector extends OneComparatorNonScoringSubCollector {

    Scorer scorer;
    
    public OneComparatorScoringMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
      initMaxScore();
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        if ((reverseMul * comparator.compareBottom(doc)) <= 0) {
          // since docs are visited in doc Id order, if compare is 0, it means
          // this document is largest than anything else in the queue, and
          // therefore not competitive.
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }

    }
    
    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over one SortField criteria, with tracking
   * document scores and maxScore, and assumes out of orderness in doc Ids
   * collection.
   */
  private class OutOfOrderOneComparatorScoringMaxScoreSubCollector extends OneComparatorScoringMaxScoreSubCollector {

    public OutOfOrderOneComparatorScoringMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        final int cmp = reverseMul * comparator.compareBottom(doc);
        if (cmp < 0 || (cmp == 0 && doc + docBase > bottom.doc)) {
          return;
        }
        
        // This hit is competitive - replace bottom element in queue & adjustTop
        comparator.copy(bottom.slot, doc);
        updateBottom(doc, score);
        comparator.setBottom(bottom.slot);
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        comparator.copy(slot, doc);
        add(slot, doc, score);
        if (queueFull) {
          comparator.setBottom(bottom.slot);
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, without
   * tracking document scores and maxScore.
   */
  private class MultiComparatorNonScoringSubCollector extends TopFieldSubCollector {
    
    final FieldComparator<?>[] comparators;
    final int[] reverseMul;

    public MultiComparatorNonScoringSubCollector(AtomicReaderContext context)
        throws IOException {
      super(context);
      comparators = queue.comparators;
      reverseMul = queue.reverseMul;
    }
    
    final void updateBottom(int doc) {
      // bottom.score is already set to Float.NaN in add().
      bottom.doc = docBase + doc;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, Float.NaN);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      // set the scorer on all comparators
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].setScorer(scorer);
      }
    }
  }
  
  /*
   * Implements a TopFieldCollector over multiple SortField criteria, without
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private class OutOfOrderMultiComparatorNonScoringSubCollector extends MultiComparatorNonScoringSubCollector {
    
    public OutOfOrderMultiComparatorNonScoringSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, Float.NaN);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore.
   */
  private class MultiComparatorScoringMaxScoreSubCollector extends MultiComparatorNonScoringSubCollector {
    
    Scorer scorer;
    
    public MultiComparatorScoringMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
      initMaxScore();
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private final class OutOfOrderMultiComparatorScoringMaxScoreSubCollector
      extends MultiComparatorScoringMaxScoreSubCollector {
    
    public OutOfOrderMultiComparatorScoringMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      final float score = scorer.score();
      if (score > maxScore) {
        maxScore = score;
      }
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore.
   */
  private class MultiComparatorScoringNoMaxScoreSubCollector extends MultiComparatorNonScoringSubCollector {
    
    Scorer scorer;
    
    public MultiComparatorScoringNoMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    final void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = queue.updateTop();
    }

    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // Here c=0. If we're at the last comparator, this doc is not
            // competitive, since docs are visited in doc Id order, which means
            // this doc cannot compete with any other document in the queue.
            return;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
  }

  /*
   * Implements a TopFieldCollector over multiple SortField criteria, with
   * tracking document scores and maxScore, and assumes out of orderness in doc
   * Ids collection.
   */
  private final class OutOfOrderMultiComparatorScoringNoMaxScoreSubCollector
      extends MultiComparatorScoringNoMaxScoreSubCollector {
    
    public OutOfOrderMultiComparatorScoringNoMaxScoreSubCollector(AtomicReaderContext context) throws IOException {
      super(context);
    }
    
    @Override
    public void collect(int doc) throws IOException {
      ++totalHits;
      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = totalHits - 1;
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        final float score = scorer.score();
        add(slot, doc, score);
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      this.scorer = scorer;
      super.setScorer(scorer);
    }
    
    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  /*
   * Implements a TopFieldCollector when after != null.
   */
  private final class PagingFieldSubCollector extends TopFieldSubCollector {

    final FieldDoc after;
    final boolean trackDocScores;
    final boolean trackMaxScore;
    final int afterDoc;

    final FieldComparator<?>[] comparators;
    final int[] reverseMul;

    Scorer scorer;

    public PagingFieldSubCollector(FieldDoc after, boolean trackDocScores, boolean trackMaxScore,
                                   AtomicReaderContext context) throws IOException {
      super(context);

      this.after = after;
      this.trackDocScores = trackDocScores;
      this.trackMaxScore = trackMaxScore;

      afterDoc = after.doc - docBase;

      comparators = queue.comparators;
      reverseMul = queue.reverseMul;

      if (trackMaxScore) {
        initMaxScore();
      }
    }

    void updateBottom(int doc, float score) {
      bottom.doc = docBase + doc;
      bottom.score = score;
      bottom = queue.updateTop();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    public void collect(int doc) throws IOException {
      totalHits++;

      //System.out.println("  collect doc=" + doc);

      // Check if this hit was already collected on a
      // previous page:
      boolean sameValues = true;
      for(int compIDX=0;compIDX<comparators.length;compIDX++) {
        final FieldComparator comp = comparators[compIDX];

        final int cmp = reverseMul[compIDX] * comp.compareDocToValue(doc, after.fields[compIDX]);
        if (cmp < 0) {
          // Already collected on a previous page
          //System.out.println("    skip: before");
          return;
        } else if (cmp > 0) {
          // Not yet collected
          sameValues = false;
          //System.out.println("    keep: after");
          break;
        }
      }

      // Tie-break by docID:
      if (sameValues && doc <= afterDoc) {
        // Already collected on a previous page
        //System.out.println("    skip: tie-break");
        return;
      }

      collectedHits++;

      float score = Float.NaN;
      if (trackMaxScore) {
        score = scorer.score();
        if (score > maxScore) {
          maxScore = score;
        }
      }

      if (queueFull) {
        // Fastmatch: return if this hit is not competitive
        for (int i = 0;; i++) {
          final int c = reverseMul[i] * comparators[i].compareBottom(doc);
          if (c < 0) {
            // Definitely not competitive.
            return;
          } else if (c > 0) {
            // Definitely competitive.
            break;
          } else if (i == comparators.length - 1) {
            // This is the equals case.
            if (doc + docBase > bottom.doc) {
              // Definitely not competitive
              return;
            }
            break;
          }
        }

        // This hit is competitive - replace bottom element in queue & adjustTop
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(bottom.slot, doc);
        }

        // Compute score only if it is competitive.
        if (trackDocScores && !trackMaxScore) {
          score = scorer.score();
        }
        updateBottom(doc, score);

        for (int i = 0; i < comparators.length; i++) {
          comparators[i].setBottom(bottom.slot);
        }
      } else {
        // Startup transient: queue hasn't gathered numHits yet
        final int slot = collectedHits - 1;
        //System.out.println("    slot=" + slot);
        // Copy hit into queue
        for (int i = 0; i < comparators.length; i++) {
          comparators[i].copy(slot, doc);
        }

        // Compute score only if it is competitive.
        if (trackDocScores && !trackMaxScore) {
          score = scorer.score();
        }
        bottom = queue.add(new Entry(slot, docBase + doc, score));
        queueFull = collectedHits == numHits;
        if (queueFull) {
          for (int i = 0; i < comparators.length; i++) {
            comparators[i].setBottom(bottom.slot);
          }
        }
      }
    }

    @Override
    public void setScorer(Scorer scorer) {
      this.scorer = scorer;
      for (int i = 0; i < comparators.length; i++) {
        comparators[i].setScorer(scorer);
      }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return true;
    }

  }

  private static interface Creator {
    TopFieldCollector create(FieldValueHitQueue<Entry> q, boolean docsScoredInOrder) throws IOException;
  }

  private static final class FakeScorer extends Scorer {
    float score;
    int doc;

    public FakeScorer() {
      super(null);
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("FakeScorer doesn't support advance(int)");
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int freq() {
      throw new UnsupportedOperationException("FakeScorer doesn't support freq()");
    }

    @Override
    public int nextDoc() {
      throw new UnsupportedOperationException("FakeScorer doesn't support nextDoc()");
    }

    @Override
    public float score() {
      return score;
    }

    @Override
    public long cost() {
      return 1;
    }
  }

  private abstract class TopFieldSubCollector implements SubCollector {

    final AtomicReaderContext context;
    final int docBase;

    final FieldValueHitQueue<Entry> queue;
    int totalHits;
    int collectedHits;
    float maxScore = Float.NaN;

    FieldValueHitQueue.Entry bottom;
    boolean queueFull;

    TopFieldSubCollector(AtomicReaderContext context) throws IOException {
      this.context = context;
      this.docBase = context.docBase;

      if (parallelized) {
        queue = FieldValueHitQueue.create(TopFieldCollector.this.queue.getFields(), numHits);
      } else { // pick up where we left of
        queue = TopFieldCollector.this.queue;
        totalHits = TopFieldCollector.this.totalHits;
        collectedHits = TopFieldCollector.this.collectedHits;
        bottom = queue.top();
        queueFull = totalHits >= numHits;
      }

      for (int i = 0; i < queue.comparators.length; i++) {
        queue.setComparator(i, queue.comparators[i].setNextReader(context));
      }
    }

    // must be invoked by subclasses that care about maxScore to correctly initialize it to a non-NaN value
    final void initMaxScore() {
      if (parallelized) {
        maxScore = Float.NEGATIVE_INFINITY;
      } else {
        maxScore = Float.isNaN(TopFieldCollector.this.maxScore)
            ? Float.NEGATIVE_INFINITY
            : Math.max(Float.NEGATIVE_INFINITY, TopFieldCollector.this.maxScore);
      }
    }

    final void add(int slot, int doc, float score) {
      bottom = queue.add(new Entry(slot, docBase + doc, score));
      queueFull = totalHits == numHits;
    }

    @Override
    public void done() throws IOException {
      if (parallelized) { // merge our state
        if (mergeAccumulator == null) {
          mergeAccumulator = recreator.create(TopFieldCollector.this.queue, false);
        }

        final FakeScorer fakeScorer = new FakeScorer();
        final SubCollector sub = mergeAccumulator.subCollector(context);
        sub.setScorer(fakeScorer);
        for (final Entry e: queue) {
          fakeScorer.doc = e.doc - docBase;
          fakeScorer.score = e.score;
          sub.collect(fakeScorer.doc);
        }
        sub.done();

        TopFieldCollector.this.totalHits += totalHits;
        TopFieldCollector.this.collectedHits += collectedHits;
        TopFieldCollector.this.maxScore = Float.isNaN(TopFieldCollector.this.maxScore)
            ? maxScore : Math.max(maxScore, TopFieldCollector.this.maxScore);
      } else { // stash our state
        TopFieldCollector.this.totalHits = totalHits;
        TopFieldCollector.this.collectedHits = collectedHits;
        TopFieldCollector.this.maxScore = maxScore;
      }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return false;
    }
  }

  private static final ScoreDoc[] EMPTY_SCOREDOCS = new ScoreDoc[0];

  final Creator recreator;

  final FieldValueHitQueue<Entry> queue;
  final int numHits;
  final boolean fillFields;

  boolean parallelized;

  int collectedHits;

  TopFieldCollector mergeAccumulator; // used during SubCollector.done() merge legwork in parallel collection

  // Stores the maximum score value encountered, needed for normalizing.
  // If document scores are not tracked, this value will stay NaN.
  float maxScore = Float.NaN;

  // Declaring the constructor private prevents extending this class by anyone
  // else. Note that the class cannot be final since it's extended by the
  // internal versions. If someone will define a constructor with any other
  // visibility, then anyone will be able to extend the class, which is not what
  // we want.
  private TopFieldCollector(Creator recreator, FieldValueHitQueue<Entry> queue, int numHits, boolean fillFields) {
    super(queue);
    this.recreator = recreator;
    this.queue = queue;
    this.numHits = numHits;
    this.fillFields = fillFields;
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   * 
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param docsScoredInOrder
   *          specifies whether documents are scored in doc Id order or not by
   *          the given {@link Scorer} in {@link SubCollector#setScorer(Scorer)}.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @throws IOException if there is a low-level I/O error
   */
  public static TopFieldCollector create(Sort sort, int numHits,
      boolean fillFields, boolean trackDocScores, boolean trackMaxScore,
      boolean docsScoredInOrder)
      throws IOException {
    return create(sort, numHits, null, fillFields, trackDocScores, trackMaxScore, docsScoredInOrder);
  }

  /**
   * Creates a new {@link TopFieldCollector} from the given
   * arguments.
   *
   * <p><b>NOTE</b>: The instances returned by this method
   * pre-allocate a full array of length
   * <code>numHits</code>.
   * 
   * @param sort
   *          the sort criteria (SortFields).
   * @param numHits
   *          the number of results to collect.
   * @param after
   *          only hits after this FieldDoc will be collected
   * @param fillFields
   *          specifies whether the actual field values should be returned on
   *          the results (FieldDoc).
   * @param trackDocScores
   *          specifies whether document scores should be tracked and set on the
   *          results. Note that if set to false, then the results' scores will
   *          be set to Float.NaN. Setting this to true affects performance, as
   *          it incurs the score computation on each competitive result.
   *          Therefore if document scores are not required by the application,
   *          it is recommended to set it to false.
   * @param trackMaxScore
   *          specifies whether the query's maxScore should be tracked and set
   *          on the resulting {@link TopDocs}. Note that if set to false,
   *          {@link TopDocs#getMaxScore()} returns Float.NaN. Setting this to
   *          true affects performance as it incurs the score computation on
   *          each result. Also, setting this true automatically sets
   *          <code>trackDocScores</code> to true as well.
   * @param docsScoredInOrder
   *          specifies whether documents are scored in doc Id order or not by
   *          the given {@link Scorer} in {@link SubCollector#setScorer(Scorer)}.
   * @return a {@link TopFieldCollector} instance which will sort the results by
   *         the sort criteria.
   * @throws IOException if there is a low-level I/O error
   */
  public static TopFieldCollector create(final Sort sort, final int numHits, final FieldDoc after,
      final boolean fillFields, final boolean trackDocScores, final boolean trackMaxScore,
      final boolean docsScoredInOrder)
      throws IOException {

    if (sort.fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    if (numHits <= 0) {
      throw new IllegalArgumentException("numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count");
    }

    final boolean needScores = trackDocScores || sort.needsScores();

    final Creator creator;
    if (after == null) {

      creator = new Creator() {
        @Override
        public TopFieldCollector create(
            final FieldValueHitQueue<Entry> queue,
            final boolean docsScoredInOrder
        ) throws IOException {
          return new TopFieldCollector(this, queue, numHits, fillFields) {
            @Override
            public SubCollector subCollector(AtomicReaderContext context) throws IOException {

              if (queue.getComparators().length == 1) {

                if (docsScoredInOrder) {
                  if (trackMaxScore) {
                    return new OneComparatorScoringMaxScoreSubCollector(context);
                  } else if (needScores) {
                    return new OneComparatorScoringNoMaxScoreSubCollector(context);
                  } else {
                    return new OneComparatorNonScoringSubCollector(context);
                  }
                } else {
                  if (trackMaxScore) {
                    return new OutOfOrderOneComparatorScoringMaxScoreSubCollector(context);
                  } else if (needScores) {
                    return new OutOfOrderOneComparatorScoringNoMaxScoreSubCollector(context);
                  } else {
                    return new OutOfOrderOneComparatorNonScoringSubCollector(context);
                  }
                }

              } else {

                // multiple comparators.
                if (docsScoredInOrder) {
                  if (trackMaxScore) {
                    return new MultiComparatorScoringMaxScoreSubCollector(context);
                  } else if (needScores) {
                    return new MultiComparatorScoringNoMaxScoreSubCollector(context);
                  } else {
                    return new MultiComparatorNonScoringSubCollector(context);
                  }
                } else {
                  if (trackMaxScore) {
                    return new OutOfOrderMultiComparatorScoringMaxScoreSubCollector(context);
                  } else if (needScores) {
                    return new OutOfOrderMultiComparatorScoringNoMaxScoreSubCollector(context);
                  } else {
                    return new OutOfOrderMultiComparatorNonScoringSubCollector(context);
                  }
                }

              }
            }
          };
        }
      };

    } else {

      if (after.fields == null) {
        throw new IllegalArgumentException("after.fields wasn't set; you must pass fillFields=true for the previous search");
      }

      if (after.fields.length != sort.getSort().length) {
        throw new IllegalArgumentException("after.fields has "
            + after.fields.length + " values but sort has " + sort.getSort().length);
      }

      creator = new Creator() {
        @Override
        public TopFieldCollector create(
            final FieldValueHitQueue<Entry> queue,
            final boolean docsScoredInOrder
        ) throws IOException {

          return new TopFieldCollector(this, queue, numHits, fillFields) {
            @Override
            public TopFieldSubCollector subCollector(AtomicReaderContext context) throws IOException {
              return new PagingFieldSubCollector(after, needScores, trackMaxScore, context);
            }
          };

        }

      };
    }

    return creator.create(FieldValueHitQueue.create(sort.fields, numHits), docsScoredInOrder);
  }

  /*
   * Only the following callback methods need to be overridden since
   * topDocs(int, int) calls them to return the results.
   */

  @Override
  protected void populateResults(ScoreDoc[] results, int howMany) {
    if (fillFields) {
      // avoid casting if unnecessary.
      for (int i = howMany - 1; i >= 0; i--) {
        results[i] = queue.fillFields(queue.pop());
      }
    } else {
      for (int i = howMany - 1; i >= 0; i--) {
        Entry entry = pq.pop();
        results[i] = new FieldDoc(entry.doc, entry.score);
      }
    }
  }
  
  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    if (results == null) {
      results = EMPTY_SCOREDOCS;
      // Set maxScore to NaN, in case this is a maxScore tracking collector.
      maxScore = Float.NaN;
    }

    // If this is a maxScoring tracking collector and there were no results, 
    return new TopFieldDocs(totalHits, results, queue.getFields(), maxScore);
  }

  @Override
  public void setParallelized() {
    parallelized = true;
  }

  @Override
  public boolean isParallelizable() {
    return true;
  }

}
