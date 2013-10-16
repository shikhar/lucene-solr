package org.apache.lucene.search;

import java.io.IOException;

public abstract class WrappingCollector implements Collector {

  public static class WrappingSubCollector implements SubCollector {

    protected final SubCollector delegate;

    public WrappingSubCollector(SubCollector delegate) {
      this.delegate = delegate;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      delegate.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
      delegate.collect(doc);
    }

    @Override
    public void done() throws IOException {
      delegate.done();
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      return delegate.acceptsDocsOutOfOrder();
    }
  }

  protected final Collector delegate;

  protected WrappingCollector(Collector delegate) {
    this.delegate = delegate;
  }

  @Override
  public void setParallelized() {
    delegate.setParallelized();
  }

  @Override
  public boolean isParallelizable() {
    return delegate.isParallelizable();
  }

}
