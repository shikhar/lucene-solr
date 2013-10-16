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
import org.apache.lucene.util.PriorityQueue;

import java.io.IOException;

public abstract class TopDocsSerialCollector<T extends ScoreDoc> extends TopDocsCollector<T> implements SubCollector {

  protected TopDocsSerialCollector(PriorityQueue<T> pq) {
    super(pq);
  }

  @Override
  public SubCollector subCollector(AtomicReaderContext context) throws IOException {
    setNextReader(context);
    return this;
  }

  @Override
  public void done() throws IOException {
  }

  @Override
  public void setParallelized() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isParallelizable() {
    return false;
  }

  /**
   * Called before collecting from each {@link AtomicReaderContext}. All doc ids in
   * {@link #collect(int)} will correspond to {@link IndexReaderContext#reader}.
   *
   * Add {@link AtomicReaderContext#docBase} to the current  {@link IndexReaderContext#reader}'s
   * internal document id to re-base ids in {@link #collect(int)}.
   *
   * @param context
   *          next atomic reader context
   */
  protected abstract void setNextReader(AtomicReaderContext context) throws IOException;
}
