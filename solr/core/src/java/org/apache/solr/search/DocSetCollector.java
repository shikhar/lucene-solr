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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SubCollector;
import org.apache.lucene.util.OpenBitSet;

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

  private OpenBitSet bits;
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
    if (smallSet != null) {
      if (size <= smallSet.length) {
        return new SortedIntDocSet(smallSet, size);
      }
      // set bits for docs that are only in the smallSet
      for (int i = 0; i < smallSet.length; i++) {
        bits.fastSet(smallSet[i]);
      }
    }
    if (bits == null) {
      return NULL_SET;
    }
    return new BitDocSet(bits, size);
  }

  private static abstract class DocSetSubCollector implements SubCollector {

    final SubCollector delegateSub;

    DocSetSubCollector(SubCollector delegateSub) {
      this.delegateSub = delegateSub;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      if (delegateSub != null) {
        delegateSub.setScorer(scorer);
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      if (delegateSub != null) {
        delegateSub.collect(doc);
      }
    }

    @Override
    public void done() throws IOException {
      if (delegateSub != null) {
        delegateSub.done();
      }
    }

  }

  @Override
  public SubCollector subCollector(AtomicReaderContext context) throws IOException {
    final SubCollector delegateSub = delegate != null ? delegate.subCollector(context) : null;
    return parallelized ? parallelSubCollector(context, delegateSub) : serialSubCollector(context, delegateSub);
  }

  private SubCollector parallelSubCollector(final AtomicReaderContext context, final SubCollector delegateSub) {
    if (bits == null) {
      bits = new OpenBitSet(maxDoc);
    }

    return new DocSetSubCollector(delegateSub) {

      long first64; // private BitSet of 64 docs that could clash with docs of another subcollector
      int size;

      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        if (context.docBase > 0 && doc < 64) {
          first64 |= 1L << doc;
        } else {
          bits.fastSet(doc + context.docBase);
        }
        size++;
      }

      @Override
      public void done() throws IOException {
        super.done();
        for (int doc = 0; first64 != 0; first64 >>>= 1, doc++) {
          if ((first64 & 1) == 1) {
            bits.fastSet(context.docBase + doc);
          }
        }
        DocSetCollector.this.size += size;
      }

      @Override
      public boolean acceptsDocsOutOfOrder() {
        return delegateSub == null || delegateSub.acceptsDocsOutOfOrder();
      }
    };
  }

  private SubCollector serialSubCollector(final AtomicReaderContext context, final SubCollector delegateSub) {
    if (smallSet == null) {
      smallSet = new int[smallSetSize];
    }

    return new DocSetSubCollector(delegateSub) {

      @Override
      public void collect(int doc) throws IOException {
        super.collect(doc);
        if (size < smallSet.length) {
          smallSet[size] = doc + context.docBase;
        } else {
          if (bits == null) {
            bits = new OpenBitSet(maxDoc);
          }
          bits.fastSet(doc + context.docBase);
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
