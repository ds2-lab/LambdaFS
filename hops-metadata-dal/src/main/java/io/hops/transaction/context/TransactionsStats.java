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
import io.hops.transaction.EntityManager;
import io.hops.transaction.handler.RequestHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static io.hops.transaction.context.EntityContextStat.HitMissCounter;
import static io.hops.transaction.context.EntityContextStat.StatsAggregator;

public class TransactionsStats {
  private static final Log LOG = LogFactory.getLog(TransactionsStats.class);
  private static TransactionsStats instance = null;

  /**
   * Clear the current statistics in preparation for handling a new request.
   */
  public void clearForServerless() throws IOException {
    LOG.debug("Clearing TransactionsStats now.");
    clear();
  }

  /**
   * Export the stats that we encountered while processing the current request.
   *
   * @param requestId The unique ID of the associated request.
   */
  public ServerlessStatisticsPackage exportForServerless(String requestId) {
    LOG.debug("Exporting " + transactionStats.size() + " transaction statistics and " +
            0 + " resolving cache statistics for request " + requestId + " now.");
            // resolvingCacheStats.size() + " resolving cache statistics for request " + requestId + " now.");
    // FOR NOW, WE ARE NEVER EXPORTING RESOLVING CACHE STATISTICS.
    return new ServerlessStatisticsPackage(requestId, transactionStats, new ArrayList<>());
  }

  /**
   * Encapsulates a package of transaction statistics for a particular
   * request/invocation of a serverless name node.
   */
  public static class ServerlessStatisticsPackage implements Serializable {
    private static final long serialVersionUID = 705940487995065547L;
    private final List<TransactionStat> transactionStats;
    private final List<ResolvingCacheStat> resolvingCacheStats;
    // private final List<EntityContextStat.StatsAggregator> statsAggregators;
    private final String requestId;

    public ServerlessStatisticsPackage(String requestId, List<TransactionStat> transactionStats,
                                       List<ResolvingCacheStat> resolvingCacheStats/*,
                                       List<EntityContextStat.StatsAggregator> statsAggregators*/) {
      this.requestId = requestId;
      this.transactionStats = transactionStats;
      this.resolvingCacheStats = resolvingCacheStats;
      // this.statsAggregators = statsAggregators;
    }

    public String getRequestId() {
      return requestId;
    }

//    public List<StatsAggregator> getStatsAggregators() {
//      return statsAggregators;
//    }

    public List<ResolvingCacheStat> getResolvingCacheStats() {
      return resolvingCacheStats;
    }

    public List<TransactionStat> getTransactionStats() {
      return transactionStats;
    }
  }

  public static class TransactionStat implements Serializable {
    private static final long serialVersionUID = -4618265598070114444L;
    private RequestHandler.OperationType name;
    private Collection<EntityContextStat> stats;
    private Exception ignoredException;

    private long acquireTime;
    private long processingTime;
    private long commitTime;

    TransactionStat(RequestHandler.OperationType name,
        Collection<EntityContextStat> stats, Exception ignoredException) {
      this.name = name;
      this.stats = stats;
      this.ignoredException = ignoredException;
    }

    public Collection<EntityContextStat> getStats() {
      return stats;
    }

    public RequestHandler.OperationType getName() {
      return name;
    }

    public Exception getIgnoredException() {
      return ignoredException;
    }

    public void setTimes(long acquire, long processing, long commit){
      this.acquireTime = acquire;
      this.processingTime = processing;
      this.commitTime = commit;
    }

    public static String getHeader(){
      String header = "Tx,";
      for(FinderType.Annotation annotation : FinderType.Annotation.values()){
        String ann = annotation.toString();
        header += ann + "_hits," + ann +"_hitsRows," + ann +"_misses," +
            ann +"_missesRows,";
      }
      header += "Hits,HitsRows,Misses,MissesRows,New,Modified,Deleted," +
          "Acquire,Processing,Commit,TotalTime";
      return header;
    }

    @Override
    public String toString(){
      String tx = name.toString() + ",";
      StatsAggregator txStatsAggregator = new StatsAggregator();

      for ( EntityContextStat contextStat : stats) {
        txStatsAggregator.update(contextStat.getStatsAggregator());
      }

      for(FinderType.Annotation annotation : FinderType.Annotation.values()){
        HitMissCounter hitMissCounter = txStatsAggregator.getCounter
            (annotation);
        tx += hitMissCounter.hits + "," + hitMissCounter.hitsRowsCount + "," +
            "" +  hitMissCounter.misses + "," + hitMissCounter
            .missesRowsCount + ",";
      }

      tx += txStatsAggregator.hitMissCounter.hits +"," + txStatsAggregator
          .hitMissCounter.hitsRowsCount +"," + txStatsAggregator
          .hitMissCounter.misses +"," + txStatsAggregator.hitMissCounter
          .missesRowsCount + ",";

      tx += txStatsAggregator.newRows + "," + txStatsAggregator.modifiedRows
          + "," + txStatsAggregator.deletedRows + ",";

      tx += acquireTime + "," + processingTime + "," + commitTime + "," +
          (acquireTime + processingTime + commitTime);
      return tx;
    }
  }


  public static class ResolvingCacheStat implements Serializable {
    private static final long serialVersionUID = -7183415931601175290L;

    public static enum Op{
      GET,
      SET
    }
    private Op operation;
    private long elapsed;
    private int roundTrips;

    public ResolvingCacheStat(Op operation, long elapsed, int roundTrips){
      this.operation = operation;
      this.elapsed = elapsed;
      this.roundTrips = roundTrips;
    }

    public static String getHeader(){
      return "Operation, Elapsed, RoundTrips";
    }
    @Override
    public String toString() {
      return operation.toString() +"," + elapsed + "," + roundTrips;
    }
  }

  private boolean enabled;
  private int WRITER_ROUND;
  private BufferedWriter statsLogWriter;
  private BufferedWriter csvFileWriter;
  private BufferedWriter memcacheCSVFileWriter;
  private List<TransactionStat> transactionStats;
  private File statsDir;
  private Thread writerThread;
  private List<ResolvingCacheStat> resolvingCacheStats;
  private boolean detailedStats;

  private TransactionsStats() {
    this.enabled = false;
    this.transactionStats = new LinkedList<>();
    this.resolvingCacheStats = new LinkedList<>();
  }

  public static TransactionsStats getInstance() {
    if (instance == null) {
      instance = new TransactionsStats();
    }
    return instance;
  }


  public void setConfiguration(boolean enableOrDisable, String statsDir, int writerRound, boolean detailed)
      throws IOException {
    if (enableOrDisable) {
      LOG.debug("Transaction Statistics is ENABLED. Detailed statistics: " + detailed);
      this.enabled = true;
      BaseEntityContext.enableStats();
      this.detailedStats = detailed;
    } else {
      LOG.debug("Transaction Statistics is DISABLED.");
      this.enabled = false;
      BaseEntityContext.disableStats();
    }
  }

  public TransactionStat collectStats(RequestHandler.OperationType operationType,
      Exception ignoredException) throws IOException {
    if (enabled) {
      Collection<EntityContextStat> contextStats =
          EntityManager.collectSnapshotStat();
      if (!contextStats.isEmpty() || ignoredException != null) {
        TransactionStat stat =
            new TransactionStat(operationType, contextStats, ignoredException);
        synchronized (transactionStats) {
          transactionStats.add(stat);
        }
        return stat;
      }
    }
    return null;
  }

  public void pushResolvingCacheStats(ResolvingCacheStat stat){
    if(enabled){
      synchronized (resolvingCacheStats){
        resolvingCacheStats.add(stat);
      }
    }
  }

  private void dump() throws IOException {
    if (enabled) {
      synchronized (transactionStats) {
        if(!transactionStats.isEmpty()) {
          dumpDetailed();
          dumpCSVLike();
          clear();
        }
      }
      synchronized (resolvingCacheStats){
        if(!resolvingCacheStats.isEmpty()) {
          dumpResolvingCacheStats();
          memcacheCSVFileWriter.flush();
          resolvingCacheStats.clear();
        }
      }
    }
  }

  public void close() throws IOException {
    if(enabled) {
      enabled = false;
//      writerThread.interrupt();
//      if(statsLogWriter != null) {
//        statsLogWriter.close();
//      }
//      if(csvFileWriter != null) {
//        csvFileWriter.close();
//      }
//      if(memcacheCSVFileWriter != null){
//        memcacheCSVFileWriter.close();
//      }
    }
  }

  public boolean isEnabled(){
    return enabled;
  }

  private void clear() throws IOException {
    if(statsLogWriter != null){
      statsLogWriter.flush();
    }
    if(csvFileWriter != null) {
      csvFileWriter.flush();
    }
    transactionStats.clear();
  }

  private void dumpResolvingCacheStats() throws IOException{
    boolean fileExists = getResolvingCacheCSVFile().exists();
    BufferedWriter writer = getResolvingCSVFileWriter();
    if(!fileExists) {
      writer.write(ResolvingCacheStat.getHeader());
      writer.newLine();
    }
    for(ResolvingCacheStat stat : resolvingCacheStats){
      writer.write(stat.toString());
      writer.newLine();
    }
  }

  private void dumpCSVLike() throws IOException{
    boolean fileExists = getCSVFile().exists();
    BufferedWriter writer = getCSVFileWriter();
    if(!fileExists) {
      writer.write(TransactionStat.getHeader());
      writer.newLine();
    }
    for(TransactionStat stat : transactionStats){
      writer.write(stat.toString());
      writer.newLine();
    }
  }

  private void dumpDetailed() throws IOException {
    if(detailedStats) {
      BufferedWriter writer = getStatsLogWriter();
      for (TransactionStat stat : transactionStats) {
        writer.write("Transaction: " + stat.name.toString());
        writer.newLine();
        dump(writer, stat);
        writer.newLine();
        writer.newLine();
      }
    }
  }

  private EntityContextStat.StatsAggregator dump(BufferedWriter writer,
      TransactionStat transactionStat) throws IOException {

    if (transactionStat.ignoredException != null) {
      writer.write(transactionStat.ignoredException.toString());
      writer.newLine();
      writer.newLine();
    }

    EntityContextStat.StatsAggregator txAggStat =
        new EntityContextStat.StatsAggregator();
    for (EntityContextStat contextStat : transactionStat.stats) {
      writer.write(contextStat.toString());
      txAggStat.update(contextStat.getStatsAggregator());
    }

    writer.write(txAggStat.toCSFString("Tx."));
    writer.newLine();
    return txAggStat;
  }

  private BufferedWriter getCSVFileWriter() throws IOException {
    if (csvFileWriter == null) {
      this.csvFileWriter = new BufferedWriter(
          new FileWriter(getCSVFile(), true));
    }
    return this.csvFileWriter;
  }


  private BufferedWriter getStatsLogWriter() throws IOException {
    if (statsLogWriter == null) {
      this.statsLogWriter = new BufferedWriter(
          new FileWriter(getStatsFile(), true));
    }
    return this.statsLogWriter;
  }

  private BufferedWriter getResolvingCSVFileWriter() throws IOException {
    if (memcacheCSVFileWriter == null) {
      this.memcacheCSVFileWriter = new BufferedWriter(
          new FileWriter(getResolvingCacheCSVFile(), true));
    }
    return this.memcacheCSVFileWriter;
  }

  private File getStatsFile() {
    return new File(statsDir, "hops-stats.log");
  }

  private File getCSVFile() {
    return new File(statsDir, "hops-stats.csv");
  }

  private File getResolvingCacheCSVFile() {
    return new File(statsDir, "hops-resolving-cache-stats.csv");
  }
}
