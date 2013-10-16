package org.apache.lucene.facet.search;

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
import java.util.IdentityHashMap;
import java.util.Map;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer.ChildScorer;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SubCollector;
import org.apache.lucene.search.Weight;

/** Collector that scrutinizes each hit to determine if it
 *  passed all constraints (a true hit) or if it missed
 *  exactly one dimension (a near-miss, to count for
 *  drill-sideways counts on that dimension). */
class DrillSidewaysCollector implements Collector {

  private final Collector hitCollector;
  private final Collector drillDownCollector;
  private final Collector[] drillSidewaysCollectors;
  private final int dimsSize;
  private final int exactCount;

  // Maps Weight to either -1 (mainQuery) or to integer
  // index of the dims drillDown.  We needs this when
  // visiting the child scorers to correlate back to the
  // right scorers:
  private final Map<Weight,Integer> weightToIndex = new IdentityHashMap<Weight,Integer>();

  public DrillSidewaysCollector(Collector hitCollector,
                                Collector drillDownCollector,
                                Collector[] drillSidewaysCollectors,
                                Map<String, Integer> dims) {
    this.hitCollector = hitCollector;
    this.drillDownCollector = drillDownCollector;
    this.drillSidewaysCollectors = drillSidewaysCollectors;
    this.dimsSize = dims.size();
    if (dimsSize == 1) {
      // When we have only one dim, we insert the
      // MatchAllDocsQuery, bringing the clause count to
      // 2:
      exactCount = 2;
    } else {
      exactCount = dimsSize;
    }
  }

  void setWeight(Weight weight, int index) {
    assert !weightToIndex.containsKey(weight);
    weightToIndex.put(weight, index);
  }

  private final class DrillSidewaysSubCollector implements SubCollector {

    private final SubCollector hitSubCollector;
    private final SubCollector drillDownSubCollector;
    private final SubCollector[] drillSideWaysSubCollectors;

    private Scorer mainScorer;
    private Scorer[] subScorers;

    private DrillSidewaysSubCollector(AtomicReaderContext leaf) throws IOException {
      hitSubCollector = hitCollector.subCollector(leaf);
      drillDownSubCollector = (drillDownCollector != null) ? drillDownCollector.subCollector(leaf) : null;
      drillSideWaysSubCollectors = new SubCollector[drillSidewaysCollectors.length];
      for (int i = 0; i < drillSidewaysCollectors.length; i++) {
        drillSideWaysSubCollectors[i] = drillSidewaysCollectors[i].subCollector(leaf);
      }
    }

    private void findScorers(Scorer scorer) {
      Integer index = weightToIndex.get(scorer.getWeight());
      if (index != null) {
        if (index.intValue() == -1) {
          mainScorer = scorer;
        } else {
          subScorers[index] = scorer;
        }
      }
      for (ChildScorer child : scorer.getChildren()) {
        findScorers(child.child);
      }
    }

    // Only used by assert:
    private boolean allMatchesFrom(int startFrom, int doc) {
      for(int i = startFrom; i < subScorers.length; i++) {
        assert subScorers[i].docID() == doc;
      }
      return true;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
      subScorers = new Scorer[dimsSize];
      findScorers(scorer);
      hitSubCollector.setScorer(scorer);
      if (drillDownSubCollector != null) {
        drillDownSubCollector.setScorer(scorer);
      }
      for (SubCollector dsc : drillSideWaysSubCollectors) {
        dsc.setScorer(scorer);
      }
    }

    @Override
    public void collect(int doc) throws IOException {
      //System.out.println("collect doc=" + doc + " main.freq=" + mainScorer.freq() + " main.doc=" + mainScorer.docID() + " exactCount=" + exactCount);

      if (mainScorer == null) {
        // This segment did not have any docs with any
        // drill-down field & value:
        return;
      }

      if (mainScorer.freq() == exactCount) {
        // All sub-clauses from the drill-down filters
        // matched, so this is a "real" hit, so we first
        // collect in both the hitCollector and the
        // drillDown collector:
        //System.out.println("  hit " + drillDownCollector);
        hitSubCollector.collect(doc);
        if (drillDownSubCollector != null) {
          drillDownSubCollector.collect(doc);
        }

        // Also collect across all drill-sideways counts so
        // we "merge in" drill-down counts for this
        // dimension.
        for(int i=0;i<subScorers.length;i++) {
          // This cannot be null, because it was a hit,
          // meaning all drill-down dims matched, so all
          // dims must have non-null scorers:
          assert subScorers[i] != null;
          int subDoc = subScorers[i].docID();
          assert subDoc == doc;
          drillSideWaysSubCollectors[i].collect(doc);
        }

      } else {
        boolean found = false;
        for(int i=0;i<subScorers.length;i++) {
          if (subScorers[i] == null) {
            // This segment did not have any docs with this
            // drill-down field & value:
            drillSideWaysSubCollectors[i].collect(doc);
            assert allMatchesFrom(i+1, doc);
            found = true;
            break;
          }
          int subDoc = subScorers[i].docID();
          //System.out.println("  i=" + i + " sub: " + subDoc);
          if (subDoc != doc) {
            //System.out.println("  +ds[" + i + "]");
            assert subDoc > doc: "subDoc=" + subDoc + " doc=" + doc;
            drillSideWaysSubCollectors[i].collect(doc);
            assert allMatchesFrom(i+1, doc);
            found = true;
            break;
          }
        }
        assert found;
      }
    }

    @Override
    public void done() throws IOException {
      hitSubCollector.done();
      if (drillDownSubCollector != null) {
        drillDownSubCollector.done();
      }
      for (int i = 0; i < drillSideWaysSubCollectors.length; i++) {
        drillSideWaysSubCollectors[i].done();
      }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
      // We actually could accept docs out of order, but, we
      // need to force BooleanScorer2 so that the
      // sub-scorers are "on" each docID we are collecting:
      return false;
    }

  }

  @Override
  public SubCollector subCollector(AtomicReaderContext leaf) throws IOException {
    return new DrillSidewaysSubCollector(leaf);
  }

  @Override
  public void setParallelized() {
    hitCollector.setParallelized();
    if (drillDownCollector != null) {
      drillDownCollector.setParallelized();
    }
    for (int i = 0; i < drillSidewaysCollectors.length; i++) {
      drillSidewaysCollectors[i].setParallelized();
    }
  }

  @Override
  public boolean isParallelizable() {
    if (!hitCollector.isParallelizable()) {
      return false;
    }
    if (drillDownCollector != null && drillDownCollector.isParallelizable()) {
      return false;
    }
    for (int i = 0; i < drillSidewaysCollectors.length; i++) {
      if (!drillSidewaysCollectors[i].isParallelizable()) {
        return false;
      }
    }
    return true;
  }
}
