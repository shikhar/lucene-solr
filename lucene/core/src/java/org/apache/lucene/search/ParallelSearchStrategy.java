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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;

/**
 * Uses an {@link ExecutorService} for executing per-{@link LeafReaderContext} search in parallel.
 */
public class ParallelSearchStrategy implements SearchStrategy {

  protected final SerialSearchStrategy serial = new SerialSearchStrategy();

  protected final Executor executor;
  protected final int parallelism;

  /**
   * Given a non-<code>null</code> {@link ExecutorService} this method runs
   * searches for each {@link LeafReaderContext} in parallel, using the provided {@link ExecutorService}.
   * <p/>
   * Shutdown for the ExecutorService should be managed externally.
   * <p/>
   * NOTE: if you are using {@link org.apache.lucene.store.NIOFSDirectory}, do not use the
   * {@link ExecutorService#shutdownNow()} method as this uses Thread.interrupt under-the-hood which can silently close
   * file descriptors (see <a href="https://issues.apache.org/jira/browse/LUCENE-2239">LUCENE-2239</a>).
   *
   * @param executor the executor service that will be utilized for parallel search
   */
  public ParallelSearchStrategy(Executor executor) {
    this(executor, Runtime.getRuntime().availableProcessors() / 2);
  }

  public ParallelSearchStrategy(Executor executor, int parallelism) {
    this.executor = executor;
    this.parallelism = parallelism;
  }

  public Executor getExecutor() {
    return executor;
  }

  @Override
  public void search(final List<LeafReaderContext> leaves, final Weight weight, final Collector collector)
      throws IOException {
    if (executor == null || !collector.isParallelizable() || leaves.size() <= 1 || parallelism < 2) {
      serial.search(leaves, weight, collector);
    } else {
      search(leaves.iterator(), weight, collector);
    }
  }

  public void search(final Iterator<LeafReaderContext> leaves, final Weight weight, final Collector collector)
      throws IOException {
    if (!leaves.hasNext()) {
      return;
    }

    collector.setParallelized();

    final List<FutureTask<LeafCollector>> pendingTasks = new ArrayList<FutureTask<LeafCollector>>();

    final Semaphore sema = new Semaphore(parallelism);

    while (leaves.hasNext()) {
      final LeafReaderContext ctx = leaves.next();

      try {
        sema.acquire();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      for (final Iterator<FutureTask<LeafCollector>> iter = pendingTasks.iterator(); iter.hasNext(); ) {
        final FutureTask<LeafCollector> task = iter.next();
        if (task.isDone()) {
          extract(task).leafDone();
          iter.remove();
        }
      }

      final LeafCollector leafCollector;
      try {
        leafCollector = collector.getLeafCollector(ctx);
      } catch (CollectionTerminatedException e) {
        continue;
      }

      final FutureTask<LeafCollector> task = new FutureTask<LeafCollector>(new Callable<LeafCollector>() {
        @Override
        public LeafCollector call() throws IOException {
          return collect(ctx, weight, leafCollector);
        }
      }) {
        @Override
        protected void done() {
          sema.release();
        }
      };
      executor.execute(task);
      pendingTasks.add(task);

    }

    for (FutureTask<LeafCollector> task : pendingTasks) {
      task.run(); // help out if it hasn't begun executing
      extract(task).leafDone();
    }
    pendingTasks.clear();

    collector.done();
  }

  protected final LeafCollector collect(LeafReaderContext ctx, Weight weight, LeafCollector leafCollector)
      throws IOException {
    BulkScorer scorer = weight.bulkScorer(ctx, !leafCollector.acceptsDocsOutOfOrder(), ctx.reader().getLiveDocs());
    if (scorer != null) {
      try {
        scorer.score(leafCollector);
      } catch (CollectionTerminatedException e) {
        // collection was terminated prematurely
        // continue with the following leaf
      }
    }
    return leafCollector;
  }

  protected final LeafCollector extract(Future<LeafCollector> f)
      throws IOException {
    LeafCollector leafCollector;
    try {
      leafCollector = f.get();
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
    return leafCollector;
  }

}