/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.transaction.context;

import com.google.common.collect.Lists;
import io.hops.exception.StorageCallPreventedException;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.metadata.hdfs.dal.AceDataAccess;
import io.hops.metadata.hdfs.entity.Ace;
import io.hops.transaction.lock.TransactionLocks;
import org.apache.hadoop.hdfs.server.namenode.ServerlessNameNode;
import org.apache.hadoop.hdfs.serverless.cache.MetadataCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AcesContext extends BaseEntityContext<Ace.PrimaryKey, Ace> {
  public static final Logger LOG = LoggerFactory.getLogger(AcesContext.class);
  
  private final AceDataAccess<Ace> dataAccess;
  private final Map<Long, List<Ace>> inodeAces = new HashMap<>();
  
  public AcesContext(AceDataAccess<Ace> aceDataAccess) {
    this.dataAccess = aceDataAccess;
  }

  private MetadataCacheManager getMetadataCacheManager() {
    ServerlessNameNode instance = ServerlessNameNode.tryGetNameNodeInstance(false);
    if (instance == null) {
      return null;
    }
    return instance.getNamesystem().getMetadataCacheManager();
  }

  /**
   * Check the metadata cache for the Ace associated with the given INode.
   */
  private List<Ace> checkCache(long inodeId, int[] aceIds) {
    if (!EntityContext.areMetadataCacheReadsEnabled()) return null;

    MetadataCacheManager metadataCacheManager = getMetadataCacheManager();
    if (metadataCacheManager == null) {
      return null;
    }

    List<Ace> aces = Lists.newArrayListWithExpectedSize(aceIds.length);
    for (int id : aceIds) {
      Ace ace = metadataCacheManager.getAce(inodeId, id);
      if (ace == null)
        return null;
      aces.add(ace);
    }

    return aces;
  }

  /**
   * Cache the Ace instances retrieved from intermediate storage in our local, in-memory metadata cache.
   *
   * The INode ID of each ace will be the same as the one we pass in. It's just for convenience.
   */
  private void cacheResults(long inodeId, List<Ace> results) {
    MetadataCacheManager metadataCacheManager = getMetadataCacheManager();
    if (metadataCacheManager == null) {
      return;
    }

    for (Ace ace : results) {
      metadataCacheManager.putAce(inodeId, ace.getIndex(), ace);
    }
  }

  @Override
  public Collection<Ace> findList(FinderType<Ace> finder, Object... params)
      throws TransactionContextException, StorageException {
    Ace.Finder aceFinder = (Ace.Finder) finder;
    switch (aceFinder){
      case ByInodeIdAndIndices:
        return findByPKBatched(aceFinder, params);
      default:
        throw new RuntimeException(UNSUPPORTED_FINDER);
    }
  }
  
  private Collection<Ace> findByPKBatched(Ace.Finder aceFinder, Object[] params)
      throws StorageException, StorageCallPreventedException {
    long inodeId = (long) params[0];
    int[] aceIds = (int[]) params[1];
    
    List<Ace> result = checkCache(inodeId, aceIds);

    if (result != null) {
      if (LOG.isDebugEnabled()) LOG.trace("Successfully retrieved all Ace instances from local cache for INode ID=" + inodeId + ".");
      return result;
    }

    if(inodeAces.containsKey(inodeId)){
      result = inodeAces.get(inodeId);
      hit(aceFinder, result, "inodeId", inodeId);
    } else {
      if (LOG.isTraceEnabled()) LOG.trace("Retrieving Aces from intermediate storage for INode ID=" + inodeId);
      aboutToAccessStorage(aceFinder, params);
      result = dataAccess.getAcesByPKBatched(inodeId, aceIds);
      inodeAces.put(inodeId, result);
      cacheResults(inodeId, result);
      gotFromDB(result);
      miss(aceFinder, result, "inodeId", inodeId);
    }
    
    return result;
  }
  
  @Override
  public void prepare(TransactionLocks tlm)
      throws TransactionContextException, StorageException {
    dataAccess.prepare(getRemoved(),getAdded(),getModified());
  }
  
  @Override
  Ace.PrimaryKey getKey(Ace ace) {
    return new Ace.PrimaryKey(ace.getInodeId(), ace.getIndex());
  }
  
  @Override
  public void update(Ace ace) throws TransactionContextException {
    super.update(ace);
    List<Ace> aces = inodeAces.get(ace.getInodeId());
    if(aces==null){
      aces = new ArrayList<>();
      inodeAces.put(ace.getInodeId(), aces);
    }
    aces.add(ace);
  }
}