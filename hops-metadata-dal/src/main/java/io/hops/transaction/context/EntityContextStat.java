/*
 * Copyright (C) 2015 hops.io.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import io.hops.metadata.common.FinderType;

import java.util.EnumMap;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;

public class EntityContextStat {

  static final int CONTEXT_INDENTATION = 2;
  static final int OPERATION_INDENTATION = 5;
  static final int CONTEXT_STAT_INDENTATION = 3;
  static final int NUMBER_WIDTH = 4;
  static final String NEW_LINE = "\n";

  static class StatsAggregator {
    HitMissCounter hitMissCounter = new HitMissCounter();
    int newRows = 0;
    int modifiedRows = 0;
    int deletedRows = 0;

    private EnumMap<FinderType.Annotation, HitMissCounter> annotated =
        new EnumMap<>(
            FinderType.Annotation.class);

    void hit(FinderType finderType, int hitRowsCount) {
      getCounter(finderType.getAnnotated()).hit(hitRowsCount);
      hitMissCounter.hit(hitRowsCount);
    }

    void miss(FinderType finderType, int missRowsCount) {
      getCounter(finderType.getAnnotated()).miss(missRowsCount);
      hitMissCounter.miss(missRowsCount);
    }

    HitMissCounter getCounter(FinderType.Annotation annotation) {
      HitMissCounter counter = annotated.get(annotation);
      if (counter == null) {
        counter = new HitMissCounter();
        annotated.put(annotation, counter);
      }
      return counter;
    }

    void update(int newRows, int modifiedRows, int deletedRows) {
      this.newRows += newRows;
      this.modifiedRows += modifiedRows;
      this.deletedRows += deletedRows;
    }

    void update(StatsAggregator statsAggregator) {
      update(statsAggregator.newRows, statsAggregator.modifiedRows,
          statsAggregator.deletedRows);
      for (EnumMap.Entry<FinderType.Annotation, HitMissCounter> e : statsAggregator.annotated
          .entrySet()) {
        getCounter(e.getKey()).update(e.getValue());
      }
      hitMissCounter.update(statsAggregator.hitMissCounter);
    }


    String getRowStats() {
      return String
          .format("N=%-" + NUMBER_WIDTH + "d M=%-" + NUMBER_WIDTH + "d " +
                  "R=%-" + NUMBER_WIDTH + "d", newRows, modifiedRows,
              deletedRows);
    }

    String getHitsMisses() {
      return hitMissCounter.toString();
    }

    String getDetailedMisses() {
      if (annotated.isEmpty()) {
        return "";
      }
      try (Formatter formatter = new Formatter()) {
        formatter.format("Detailed Misses: ");
        for (EnumMap.Entry<FinderType.Annotation, HitMissCounter> e : annotated
            .entrySet()) {
          String onlyMisses = e.getValue().onlyMisses();
          if (!onlyMisses.isEmpty()) {
            formatter.format("%s %s ", e.getKey().getShort(), onlyMisses);
          }
        }
        return formatter.toString();
      }
    }

    boolean isEmpty() {
      return newRows == 0 && modifiedRows == 0 && deletedRows == 0;
    }

    String toString(String prefix) {
      StringBuilder sb = new StringBuilder();
      sb.append(prefix + "  " + getRowStats() + NEW_LINE);
      String hitMisses = getHitsMisses();
      if (!hitMisses.isEmpty()) {
        sb.append(prefix + "  " + hitMisses + NEW_LINE);
      }
      String detailedMisses = getDetailedMisses();
      if (!detailedMisses.isEmpty()) {
        sb.append(prefix + "  " + detailedMisses + NEW_LINE);
      }
      return sb.toString();
    }

    String toCSFString(String prefix) {
      StringBuilder sb = new StringBuilder();
      sb.append(getCSF(prefix + "  " + getRowStats()));
      String hitMisses = getHitsMisses();
      if (!hitMisses.isEmpty()) {
        sb.append(getCSF(prefix + "  " + hitMisses));
      }
      String detailedMisses = getDetailedMisses();
      if (!detailedMisses.isEmpty()) {
        sb.append(getCSF(prefix + "  " + detailedMisses));
      }
      return sb.toString();
    }

    @Override
    public String toString() {
      return toString("");
    }
  }

  static class HitMissCounter {
    int hits;
    int hitsRowsCount;
    int misses;
    int missesRowsCount;

    void hit(int hitsRowsCount) {
      hit(1, hitsRowsCount);
    }

    void miss(int missesRowsCount) {
      miss(1, missesRowsCount);
    }

    void hit(int hits, int hitsRowsCount) {
      this.hits += hits;
      this.hitsRowsCount += hitsRowsCount;
    }

    void miss(int misses, int missesRowsCount) {
      this.misses += misses;
      this.missesRowsCount += missesRowsCount;
    }

    void update(HitMissCounter other) {
      hit(other.hits, other.hitsRowsCount);
      miss(other.misses, other.missesRowsCount);
    }

    String onlyMisses() {
      if (misses == 0) {
        return "";
      }
      return String.format("%d(%d)", misses, missesRowsCount);
    }

    @Override
    public String toString() {
      if (hits == 0 && misses == 0) {
        return "";
      }
      return String.format("Hits=%d(%d) Misses=%d(%d)" +
          (misses > hits ? " MORE DATA THAN NEEDED" : ""), hits, hitsRowsCount,
          misses, missesRowsCount);
    }
  }

  private final String contextName;
  private final Map<FinderType, HitMissCounter> operationsStats;
  private StatsAggregator statsAggregator;

  public EntityContextStat(String contextName) {
    this.contextName = contextName;
    this.operationsStats = new HashMap<>();
    this.statsAggregator = new StatsAggregator();
  }

  public void hit(FinderType finder, int count) {
    getCounter(finder).hit(count);
    statsAggregator.hit(finder, count);
  }

  public void miss(FinderType finder, int count) {
    getCounter(finder).miss(count);
    statsAggregator.miss(finder, count);
  }

  private HitMissCounter getCounter(FinderType finder) {
    HitMissCounter counter = operationsStats.get(finder);
    if (counter == null) {
      counter = new HitMissCounter();
      operationsStats.put(finder, counter);
    }
    return counter;
  }


  public void commited(int newRows, int modifiedRows, int deletedRows) {
    statsAggregator.update(newRows, modifiedRows, deletedRows);
  }

  public String getContextName() {
    return contextName;
  }


  boolean isEmpty() {
    return statsAggregator.isEmpty() && operationsStats.isEmpty();
  }

  StatsAggregator getStatsAggregator() {
    return statsAggregator;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getCF("----------------------------------------"));
    sb.append(getCF(contextName));

    for (Map.Entry<FinderType, HitMissCounter> e : operationsStats.entrySet()) {
      sb.append(
          getOPF("%s[%s] H=%-" + NUMBER_WIDTH + "d M=%-" + NUMBER_WIDTH + "d",
              e.getKey(), e.getKey().getAnnotated().getShort(),
              e.getValue().hits, e.getValue().misses));
    }

    sb.append(getCSF(statsAggregator.getRowStats()));
    sb.append(getCSF(statsAggregator.getHitsMisses()));

    String detailedMisses = statsAggregator.getDetailedMisses();
    if (!detailedMisses.isEmpty()) {
      sb.append(getCSF(detailedMisses));
    }
    sb.append(getCF("----------------------------------------"));
    return sb.toString();
  }

  static String getOPF(String format, Object... params) {
    return String.format(getStringSpacing(OPERATION_INDENTATION) + format +
        NEW_LINE, prefix("", params));
  }

  static String getCSF(String format, Object... params) {
    return String.format(getStringSpacing(CONTEXT_STAT_INDENTATION) + format +
        NEW_LINE, prefix("", params));
  }

  static String getCF(String format, Object... params) {
    return String.format(getStringSpacing(CONTEXT_INDENTATION) + format +
        NEW_LINE, prefix("", params));
  }

  private static Object[] prefix(Object p, Object[] params) {
    Object[] res = new Object[params.length + 1];
    res[0] = p;
    System.arraycopy(params, 0, res, 1, params.length);
    return res;
  }

  private static String getStringSpacing(int spaces) {
    return "%" + spaces + "s ";
  }

}
