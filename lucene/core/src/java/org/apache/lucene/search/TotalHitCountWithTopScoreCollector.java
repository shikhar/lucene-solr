package org.apache.lucene.search;

import org.apache.lucene.index.AtomicReaderContext;

import java.io.IOException;

public class TotalHitCountWithTopScoreCollector implements Collector {

  private final boolean scoringEnabled;
  private int totalHits = 0;
  private float topScore = Float.NEGATIVE_INFINITY;

  public TotalHitCountWithTopScoreCollector(boolean scoringEnabled) {
    this.scoringEnabled = scoringEnabled;
  }

  public int getTotalHits() {
    return totalHits;
  }

  public float getTopScore() {
    return topScore;
  }

  @Override
  public SubCollector subCollector(AtomicReaderContext ctx) throws IOException {
    return new SubCollector() {

      private int totalHits = 0;
      private float topScore = Float.NEGATIVE_INFINITY;
      private Scorer scorer;

      @Override
      public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        totalHits++;
        if (scoringEnabled) {
          float score = scorer.score();
          if (score > topScore) {
            topScore = score;
          }
        }
      }

      @Override
      public void done() throws IOException {
        TotalHitCountWithTopScoreCollector.this.totalHits += totalHits;
        if (scoringEnabled) {
          if (TotalHitCountWithTopScoreCollector.this.topScore > topScore) {
            TotalHitCountWithTopScoreCollector.this.topScore = topScore;
          }
        }
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return true;
      }
    };
  }

  @Override
  public void setParallelized() {
  }

  @Override
  public boolean isParallelizable() {
    return true;
  }

}
