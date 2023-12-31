/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.fs.StorageType;
import io.hops.exception.StorageException;
import io.hops.exception.TransactionContextException;
import io.hops.metadata.common.FinderType;
import io.hops.transaction.EntityManager;
import org.apache.hadoop.hdfs.protocol.DSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.protocol.QuotaByStorageTypeExceededException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.util.EnumCounters;

import java.io.Serializable;

/**
 * Quota feature for {@link INodeDirectory}
 */
public class DirectoryWithQuotaFeature implements INode.Feature, Serializable {

  private static final long serialVersionUID = 7853052195523598736L;

  public static enum Finder implements FinderType<DirectoryWithQuotaFeature> {

    ByINodeId,
    ByINodeIds;

    @Override
    public Class getType() {
      return DirectoryWithQuotaFeature.class;
    }

    @Override
    public Annotation getAnnotated() {
      switch (this) {
        case ByINodeId:
          return Annotation.PrimaryKey;
        case ByINodeIds:
          return Annotation.Batched;
        default:
          throw new IllegalStateException();
      }
    }

  }
  
  public static final long DEFAULT_NAMESPACE_QUOTA = Long.MAX_VALUE;
  public static final long DEFAULT_STORAGE_SPACE_QUOTA = HdfsConstants.QUOTA_RESET;

  private QuotaCounts quota;
  private QuotaCounts usage;
  private Long inodeId;
  
  public static class Builder {
    private QuotaCounts quota;
    private QuotaCounts usage;
    private Long inodeId;

    public Builder(Long inodeId) {
      this.inodeId = inodeId;
      this.quota = new QuotaCounts.Builder().nameSpace(DEFAULT_NAMESPACE_QUOTA).
          storageSpace(DEFAULT_STORAGE_SPACE_QUOTA).
          typeSpaces(DEFAULT_STORAGE_SPACE_QUOTA).build();
      this.usage = new QuotaCounts.Builder().nameSpace(1).build();
    }

    public Builder nameSpaceQuota(long nameSpaceQuota) {
      this.quota.setNameSpace(nameSpaceQuota);
      return this;
    }

    public Builder storageSpaceQuota(long spaceQuota) {
      this.quota.setStorageSpace(spaceQuota);
      return this;
    }

    public Builder typeQuotas(EnumCounters<StorageType> typeQuotas) {
      this.quota.setTypeSpaces(typeQuotas);
      return this;
    }

    public Builder typeQuota(StorageType type, long quota) {
      this.quota.setTypeSpace(type, quota);
      return this;
    }

    public Builder nameSpaceUsage(long nameSpaceQuota) {
      this.usage.setNameSpace(nameSpaceQuota);
      return this;
    }

    public Builder spaceUsage(long spaceQuota) {
      this.usage.setStorageSpace(spaceQuota);
      return this;
    }

    public Builder typeUsages(EnumCounters<StorageType> typeQuotas) {
      this.usage.setTypeSpaces(typeQuotas);
      return this;
    }
    
    public Builder typeUsage(StorageType type, long quota) {
      this.usage.setTypeSpace(type, quota);
      return this;
    }
    
    public DirectoryWithQuotaFeature build(){
      return new DirectoryWithQuotaFeature(this);
    }
    
    public DirectoryWithQuotaFeature build(boolean save) throws TransactionContextException, StorageException {
      if (save) {
        return new DirectoryWithQuotaFeature(this, save);
      } else {
        return build();
      }
    }
  }

    private DirectoryWithQuotaFeature(Builder builder) {
    this.inodeId = builder.inodeId;
    this.quota = builder.quota;
    this.usage = builder.usage;
  }

  
  private DirectoryWithQuotaFeature(Builder builder, boolean save) throws TransactionContextException, StorageException {
    this.inodeId = builder.inodeId;
    this.quota = builder.quota;
    this.usage = builder.usage;
    if(save){
      save();
    }
  }

  /** @return the quota set or null if it is not set. */
  public QuotaCounts getQuota() {
    return new QuotaCounts.Builder().quotaCount(this.quota).build();
  }
  
  /**
   * Set the directory's quota
   *
   * @param nsQuota Namespace quota to be set
   * @param ssQuota Storagespace quota to be set
   * @param type Storage type of the storage space quota to be set.
   *             To set storagespace/namespace quota, type must be null.
   */
  void setQuota(long nsQuota, long ssQuota, StorageType type) 
      throws StorageException, TransactionContextException {
    if (type != null) {
      this.quota.setTypeSpace(type, ssQuota);
    } else {
      setQuota(nsQuota, ssQuota);
    }
    save();
  }

  void setQuota(long nsQuota, long ssQuota) throws TransactionContextException, StorageException {
    this.quota.setNameSpace(nsQuota);
    this.quota.setStorageSpace(ssQuota);
    save();
  }

  void setQuota(long quota, StorageType type) throws TransactionContextException, StorageException {
    this.quota.setTypeSpace(type, quota);
    save();
  }

  /** Set storage type quota in a batch. (Only used by FSImage load)
   *
   * @param tsQuotas type space counts for all storage types supporting quota
   */
  void setQuota(EnumCounters<StorageType> tsQuotas) throws TransactionContextException, StorageException {
    this.quota.setTypeSpaces(tsQuotas);
    save();
  }

  /**
   * Add current quota usage to counts and return the updated counts
   * @param counts counts to be added with current quota usage
   * @return counts that have been added with the current qutoa usage
   */
  QuotaCounts AddCurrentSpaceUsage(QuotaCounts counts) {
    counts.add(this.usage);
    return counts;
  }
  
  ContentSummaryComputationContext computeContentSummary(final INodeDirectory dir,
      final ContentSummaryComputationContext summary)
    throws StorageException, TransactionContextException {
    final long original = summary.getCounts().getStoragespace();
    long oldYieldCount = summary.getYieldCount();
    dir.computeDirectoryContentSummary(summary);
    // Check only when the content has not changed in the middle.
    if (oldYieldCount == summary.getYieldCount()) {
      checkStoragespace(dir, summary.getCounts().getStoragespace() - original);
    }
    return summary;
  }

  private void checkStoragespace(final INodeDirectory dir, final long computed) 
      throws StorageException, TransactionContextException {
    if (-1 != quota.getStorageSpace() && usage.getStorageSpace() != computed) {
      ServerlessNameNode.LOG.error("BUG: Inconsistent storagespace for directory "
          + dir.getFullPathName() + ". Cached = " + usage.getStorageSpace()
          + " != Computed = " + computed);
    }
  }
  
  /**
   * Update the size of the tree
   *
   * @param dir Directory i-node
   * @param nsDelta the change of the tree size
   * @param dsDelta change to diskspace occupied
   * @throws StorageException
   * @throws TransactionContextException
   */
  void addSpaceConsumed(final INodeDirectory dir, final QuotaCounts counts)
    throws StorageException, TransactionContextException {
    if (dir.isQuotaSet()) {
      // The following steps are important:
      // check quotas in this inode and all ancestors before changing counts
      // so that no change is made if there is any quota violation.
      // (1) verify quota in this inode
//      if (verify) {
//        verifyQuota(counts);
//      }
      // (2) verify quota and then add count in ancestors
      //dir.addSpaceConsumed2Parent(counts);
      // (3) add count in this inode
      addSpaceConsumed2Cache(counts);
    } 
  }
  
  /** Update the space/namespace/type usage of the tree
   * @param delta the change of the namespace/space/type usage
   */
  public void addSpaceConsumed2Cache(QuotaCounts delta) throws TransactionContextException, StorageException {
    usage.add(delta);
    save();
  }

  /** 
   * Sets namespace and storagespace take by the directory rooted
   * at this INode. This should be used carefully. It does not check 
   * for quota violations.
   *
   * @param dir Directory i-node
   * @param namespace size of the directory to be set
   * @param storagespace storage space take by all the nodes under this directory
   * @param typespaces counters of storage type usage
   */
  void setSpaceConsumed(long namespace, long storagespace,
      EnumCounters<StorageType> typespaces) {
    usage.setNameSpace(namespace);
    usage.setStorageSpace(storagespace);
    usage.setTypeSpaces(typespaces);
  }
  
  void setSpaceConsumed(QuotaCounts c) {
    usage.setNameSpace(c.getNameSpace());
    usage.setStorageSpace(c.getStorageSpace());
    usage.setTypeSpaces(c.getTypeSpaces());
  }

  /** @return the namespace and storagespace and typespace consumed. */
  public QuotaCounts getSpaceConsumed() {
    return new QuotaCounts.Builder().quotaCount(usage).build();
  }
  
  /** Verify if the namespace quota is violated after applying delta. */
  private void verifyNamespaceQuota(long delta) throws NSQuotaExceededException {
    if (Quota.isViolated(quota.getNameSpace(), usage.getNameSpace(), delta)) {
      throw new NSQuotaExceededException(quota.getNameSpace(),
          usage.getNameSpace() + delta);
    }
  }
  /** Verify if the storagespace quota is violated after applying delta. */
  private void verifyStoragespaceQuota(long delta) throws DSQuotaExceededException {
    if (Quota.isViolated(quota.getStorageSpace(), usage.getStorageSpace(), delta)) {
      throw new DSQuotaExceededException(quota.getStorageSpace(),
          usage.getStorageSpace() + delta);
    }
  }
  
  private void verifyQuotaByStorageType(EnumCounters<StorageType> typeDelta)
      throws QuotaByStorageTypeExceededException {
    if (!isQuotaByStorageTypeSet()) {
      return;
    }
    for (StorageType t: StorageType.getTypesSupportingQuota()) {
      if (!isQuotaByStorageTypeSet(t)) {
        continue;
      }
      if (Quota.isViolated(quota.getTypeSpace(t), usage.getTypeSpace(t),
          typeDelta.get(t))) {
        throw new QuotaByStorageTypeExceededException(
          quota.getTypeSpace(t), usage.getTypeSpace(t) + typeDelta.get(t), t);
      }
    }
  }
  /**
   * @throws QuotaExceededException if namespace, storagespace or storage type
   * space quota is violated after applying the deltas.
   */
  void verifyQuota(QuotaCounts counts)
    throws QuotaExceededException, StorageException, TransactionContextException {
    verifyNamespaceQuota(counts.getNameSpace());
    verifyStoragespaceQuota(counts.getStorageSpace());
    verifyQuotaByStorageType(counts.getTypeSpaces());
  }
  
  boolean isQuotaSet() {
    return quota.anyNsSsCountGreaterOrEqual(0) ||
        quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet() {
    return quota.anyTypeSpaceCountGreaterOrEqual(0);
  }

  boolean isQuotaByStorageTypeSet(StorageType t) {
    return quota.getTypeSpace(t) >= 0;
  }

  private String namespaceString() {
    return "namespace: " + (quota.getNameSpace() < 0? "-":
        usage.getNameSpace() + "/" + quota.getNameSpace());
  }
  private String storagespaceString() {
    return "storagespace: " + (quota.getStorageSpace() < 0? "-":
        usage.getStorageSpace() + "/" + quota.getStorageSpace());
  }

  private String typeSpaceString() {
    StringBuilder sb = new StringBuilder();
    for (StorageType t : StorageType.getTypesSupportingQuota()) {
      sb.append("StorageType: " + t +
          (quota.getTypeSpace(t) < 0? "-":
          usage.getTypeSpace(t) + "/" + usage.getTypeSpace(t)));
    }
    return sb.toString();
  }

  @Override
  public String toString() {
    return "Quota[" + namespaceString() + ", " + storagespaceString() +
        ", " + typeSpaceString() + "]";
  }
  
  void remove() throws StorageException, TransactionContextException{
    EntityManager.remove(this);
  }
  
  void save() throws TransactionContextException, StorageException{
    EntityManager.update(this);
  }

  public Long getInodeId() {
    return inodeId;
  }
}
