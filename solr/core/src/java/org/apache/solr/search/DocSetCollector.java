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
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.util.FixedBitSet;

import java.io.IOException;

public class DocSetCollector implements Collector {

  private static final SortedIntDocSet NULL_SET = new SortedIntDocSet(new int[0], 0);

  private final int smallSetSize;
  private final int maxDoc;
  private final Collector delegate;

  private boolean parallelized;

  // In case there aren't that many hits, we may not want a very sparse bit array.
  // Optimistically collect the first few docs in an array in case there are only a few.
  private int[] smallSet;

  private FixedBitSet bits;
  private int size;

  public DocSetCollector(int smallSetSize, int maxDoc) {
    this(smallSetSize, maxDoc, null);
  }

  public DocSetCollector(int smallSetSize, int maxDoc, Collector delegate) {
    this.smallSetSize = smallSetSize;
    this.maxDoc = maxDoc;
    this.delegate = delegate;
  }

  public DocSet getDocSet() {
    if (smallSet == null) {
      if (bits == null) {
        return NULL_SET;
      } else {
        return new BitDocSet(bits, size);
      }
    } else {
      if (bits == null) {
        return new SortedIntDocSet(smallSet, size);
      } else {
        // set bits for docs that are only in the smallSet
        for (int i = 0; i < smallSet.length; i++) {
          bits.set(smallSet[i]);
        }
        return new BitDocSet(bits, size);
      }
    }
  }

  private static abstract class DocSetLeafCollector implements LeafCollector {

    final LeafCollector leafDelegate;

    DocSetLeafCollector(LeafCollector leafDelegate) {
      this.leafDelegate = leafDelegate;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      if (leafDelegate != null) {
        leafDelegate.setScorer(scorer);
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      if (leafDelegate != null) {
        leafDelegate.collect(doc);
      }
    }

    @Override
    public void leafDone() throws IOException {
      if (leafDelegate != null) {
        leafDelegate.leafDone();
      }
    }

  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    final LeafCollector leafDelegate = delegate != null ? delegate.getLeafCollector(context) : null;
    return parallelized ? parallelLeafCollector(context, leafDelegate) : serialLeafCollector(context, leafDelegate);
  }

  @Override
  public void done() throws IOException {
  }

  private LeafCollector parallelLeafCollector(final LeafReaderContext context, final LeafCollector leafDelegate) {
    if (bits == null) {
      bits = new FixedBitSet(maxDoc);
    }

    return new DocSetLeafCollector(leafDelegate) {
      final int docBase = context.docBase;
      final int last64Threshold = context.reader().maxDoc() - 64;

      long first64; // private BitSet of 64 docs that could clash with docs of prev leaf
      long last64; // private BitSet of 64 docs that could clash with docs of next leaf
      int size;

      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        if (docBase > 0 && doc < 64) {
          first64 |= 1L << doc;
        } else if (doc >= last64Threshold) {
          last64 |= 1L << doc;
        } else {
          bits.set(doc + docBase);
        }
        size++;
      }

      @Override
      public void leafDone() throws IOException {
        super.leafDone();
        mergeBoundaryDocs(first64);
        mergeBoundaryDocs(last64);
        DocSetCollector.this.size += size;
      }

      private void mergeBoundaryDocs(long boundaryDocs) {
        for (int doc = 0; boundaryDocs != 0; boundaryDocs >>>= 1, doc++) {
          if ((boundaryDocs & 1) == 1) {
            bits.set(docBase + doc);
          }
        }
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return leafDelegate == null || leafDelegate.acceptsDocsOutOfOrder();
      }
    };
  }

  private LeafCollector serialLeafCollector(final LeafReaderContext context, final LeafCollector leafDelegate) {
    if (smallSet == null) {
      smallSet = new int[smallSetSize];
    }

    return new DocSetLeafCollector(leafDelegate) {

      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        if (size < smallSet.length) {
          smallSet[size] = doc + context.docBase;
        } else {
          if (bits == null) {
            bits = new FixedBitSet(maxDoc);
          }
          bits.set(doc + context.docBase);
        }
        size++;
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return false; // smallSet needs to be collected in sorted order
      }

    };
  }

  @Override
  public void setParallelized() {
    if (delegate != null) {
      delegate.setParallelized();
    }
    parallelized = true;
  }

  @Override
  public boolean isParallelizable() {
    return delegate == null || delegate.isParallelizable();
  }

}