/*
 * Hops Database abstraction layer for storing the hops metadata in MySQL Cluster
 * Copyright (C) 2015  hops.io
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.*;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.HashBucketDataAccess;
import io.hops.metadata.hdfs.entity.HashBucket;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;

import java.util.*;

public class HashBucketClusterj
    implements TablesDef.HashBucketsTableDef, HashBucketDataAccess<HashBucket> {

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = STORAGE_ID)
  @Index(name = "storage_idx")
  public interface HashBucketDTO {
    
    @PrimaryKey
    @Column(name = STORAGE_ID)
    int getStorageId();

    void setStorageId(int storageId);
    
    @PrimaryKey
    @Column(name = BUCKET_ID)
    int getBucketId();

    void setBucketId(int bucketId);
    
    @Column(name = HASH)
    byte[] getHash();

    void setHash(byte[] hash);
  }

  @Override
  public HashBucket findBucket(int storageId, int bucketId) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    Object[] pk = new Object[2];
    pk[0] = storageId;
    pk[1] = bucketId;

    HashBucketDTO result = session.find(HashBucketDTO.class, pk);
    if (result != null) {
      HashBucket hashBucket = convert(result);
      session.release(result);
      return hashBucket;
    } else {
      return null;
    }
  }

  @Override
  public Collection<HashBucket> findBucketsByStorageId(int storageId) throws
      StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<HashBucketDTO> dobj =
            qb.createQueryDefinition(HashBucketDTO.class);
    HopsPredicate pred1 = dobj.get("storageId").equal(dobj.param("storageIdParam"));
    dobj.where(pred1);
    HopsQuery<HashBucketDTO> query = session.createQuery(dobj);
    query.setParameter("storageIdParam", storageId);
    return convertAndRelease(session, query.getResultList());
  }
  
  @Override
  public void prepare(Collection<HashBucket> removed,
      Collection<HashBucket> modified) throws StorageException {
    
    List<HashBucketDTO> changes = new ArrayList<>();
    List<HashBucketDTO> deletions = new ArrayList<>();
    HopsSession session = connector.obtainSession();
    for (HashBucket hashBucket : removed) {
      HashBucketDTO newInstance = session.newInstance(HashBucketDTO.class);
      createPersistable(hashBucket, newInstance);
      deletions.add(newInstance);
    }

    for (HashBucket hashBucket : modified) {
      HashBucketDTO newInstance = session.newInstance(HashBucketDTO.class);
      createPersistable(hashBucket, newInstance);
      changes.add(newInstance);
    }
    session.deletePersistentAll(deletions);
    session.savePersistentAll(changes);

    session.release(deletions);
    session.release(changes);
  }

  private HashBucket convert(HashBucketDTO result) {
    return new HashBucket(result.getStorageId(),result.getBucketId(),
        result.getHash());
  }

  private List<HashBucket> convertAndRelease(HopsSession session,
      List<HashBucketDTO> triplets) throws StorageException {
    List<HashBucket> hashBuckets = new ArrayList<>(triplets.size());
    for (HashBucketDTO t : triplets) {
      hashBuckets.add(convert(t));
      session.release(t);
    }
    return hashBuckets;
  }

  private void createPersistable(HashBucket hashBucket, HashBucketDTO newInstance) {
    newInstance.setStorageId(hashBucket.getStorageId());
    newInstance.setBucketId(hashBucket.getBucketId());
    newInstance.setHash(hashBucket.getHash());
  }
}
