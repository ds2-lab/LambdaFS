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

import com.google.common.primitives.Longs;
import com.mysql.clusterj.LockMode;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.Index;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.INodeDataAccess;
import io.hops.metadata.hdfs.entity.INode;
import io.hops.metadata.hdfs.entity.INodeIdentifier;
import io.hops.metadata.hdfs.entity.INodeMetadataLogEntry;
import io.hops.metadata.hdfs.entity.ProjectedINode;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.NdbBoolean;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.mysqlserver.MysqlServerConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import io.hops.transaction.context.EntityContext;

import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class INodeClusterj implements TablesDef.INodeTableDef, INodeDataAccess<INode> {
  public static final Log LOG = LogFactory.getLog(INodeClusterj.class);
  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = PARTITION_ID)
  public interface InodeDTO {
    @PrimaryKey
    @Column(name = PARTITION_ID)
    long getPartitionId();
    void setPartitionId(long partitionId);

    //id of the parent inode
    @PrimaryKey
    @Column(name = PARENT_ID)
    @Index(name = "pidex")
    long getParentId();     // id of the inode
    void setParentId(long parentid);

    @PrimaryKey
    @Column(name = NAME)
    String getName();     //name of the inode
    void setName(String name);

    @Column(name = ID)
    @Index(name = "inode_idx")
    long getId();     // id of the inode
    void setId(long id);

    @Column(name = IS_DIR)
    byte getIsDir();
    void setIsDir(byte isDir);

    // Inode
    @Column(name = MODIFICATION_TIME)
    long getModificationTime();
    void setModificationTime(long modificationTime);

    // Inode
    @Column(name = ACCESS_TIME)
    long getATime();
    void setATime(long modificationTime);

    // Inode
    @Column(name = USER_ID)
    int getUserID();
    void setUserID(int userID);

    // Inode
    @Column(name = GROUP_ID)
    int getGroupID();
    void setGroupID(int groupID);

    // Inode
    @Column(name = PERMISSION)
    short getPermission();
    void setPermission(short permission);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_NAME)
    String getClientName();
    void setClientName(String isUnderConstruction);

    // InodeFileUnderConstruction
    @Column(name = CLIENT_MACHINE)
    String getClientMachine();
    void setClientMachine(String clientMachine);

    //  marker for InodeFile
    @Column(name = GENERATION_STAMP)
    int getGenerationStamp();
    void setGenerationStamp(int generation_stamp);

    // InodeFile
    @Column(name = HEADER)
    long getHeader();
    void setHeader(long header);

    //INodeSymlink
    @Column(name = SYMLINK)
    String getSymlink();
    void setSymlink(String symlink);

    @Column(name = QUOTA_ENABLED)
    byte getQuotaEnabled();
    void setQuotaEnabled(byte quotaEnabled);

    @Column(name = UNDER_CONSTRUCTION)
    byte getUnderConstruction();
    void setUnderConstruction(byte underConstruction);

    @Column(name = SUBTREE_LOCKED)
    byte getSubtreeLocked();
    void setSubtreeLocked(byte locked);

    @Column(name = SUBTREE_LOCK_OWNER)
    long getSubtreeLockOwner();
    void setSubtreeLockOwner(long leaderId);

    @Column(name = META_ENABLED)
    byte getMetaEnabled();
    void setMetaEnabled(byte metaEnabled);

    @Column(name = SIZE)
    long getSize();
    void setSize(long size);

    @Column(name = FILE_STORED_IN_DB)
    byte getFileStoredInDd();
    void setFileStoredInDd(byte isFileStoredInDB);

    @Column(name = LOGICAL_TIME)
    int getLogicalTime();
    void setLogicalTime(int logicalTime);

    @Column(name = STORAGE_POLICY)
    byte getStoragePolicy();
    void setStoragePolicy(byte storagePolicy);
    
    @Column(name = CHILDREN_NUM)
    int getChildrenNum();
    void setChildrenNum(int childrenNum);
    
    @Column(name = NUM_ACES)
    int getNumAces();
    void setNumAces(int numAces);
  
    @Column(name = NUM_USER_XATTRS)
    byte getNumUserXAttrs();
    void setNumUserXAttrs(byte numUserXAttrs);
  
    @Column(name = NUM_SYS_XATTRS)
    byte getNumSysXAttrs();
    void setNumSysXAttrs(byte numSysXAttrs);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private MysqlServerConnector mysqlConnector =
      MysqlServerConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public void prepare(Collection<INode> removed, Collection<INode> newEntries,
      Collection<INode> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> changes = new ArrayList<>();
    List<InodeDTO> deletions = new ArrayList<>();
    try {
      for (INode inode : removed) {
        Object[] pk = new Object[3];
        pk[0] = inode.getPartitionId();
        pk[1] = inode.getParentId();
        pk[2] = inode.getName();
        InodeDTO persistable = session.newInstance(InodeDTO.class, pk);
        deletions.add(persistable);
      }

      for (INode inode : newEntries) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        changes.add(persistable);
      }

      for (INode inode : modified) {
        InodeDTO persistable = session.newInstance(InodeDTO.class);
        createPersistable(inode, persistable);
        changes.add(persistable);
      }

      if(!deletions.isEmpty()) {
        session.deletePersistentAll(deletions);
        session.flush();
      }

      session.savePersistentAll(changes);
    }finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public INode findInodeByIdFTIS(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("id").equal(dobj.param("idParam"));
    dobj.where(pred1);

    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", inodeId);

    List<InodeDTO> results = null;

    try {
      results = query.getResultList();
      if (results.size() > 1) {
        throw new StorageException("Fetching inode by id:" + inodeId + ". Only one record was expected. Found: " + results.size());
      }

      if (results.size() == 1) {
        INode inode = convert(results.get(0));
        return inode;
      } else {
        return null;
      }
    }finally {
      session.release(results);
    }
  }

  @Override
  public Collection<INode> findInodesByIdsFTIS(long[] inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("id").in(dobj.param("idParam"));
    dobj.where(pred1);

    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("idParam", Longs.asList(inodeId));

    List<InodeDTO> results = null;

    try {
      results = query.getResultList();
      if (results.isEmpty()) {
        return null;
      }

      return convert(results);
    }finally {
      session.release(results);
    }
  }
  
  @Override
  public List<INode> findInodesByParentIdFTIS(long parentId)
      throws StorageException {
    //System.out.println("*** indexScanFindInodesByParentId ");
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 =
        dobj.get("parentId").equal(dobj.param("parentIDParam"));
    dobj.where(pred1);
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("parentIDParam", parentId);

    List<InodeDTO> results = null;
    try {
      results = query.getResultList();
      List<INode> inodeList = convert(results);
      return inodeList;
    }finally {
      session.release(results);
    }
  }

  @Override
  public List<INode> findInodesByParentIdAndPartitionIdPPIS(long parentId, long partitionId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
    dobj.where(pred1.and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("partitionIDParam", partitionId);
    query.setParameter("parentIDParam", parentId);

    List<InodeDTO> results = null;
    try {
      results = query.getResultList();
      List<INode> inodeList = convert(results);
      return inodeList;
    }finally{
      session.release(results);
    }
  }

  @Override
  public List<ProjectedINode> findInodesFTISTx(
      long parentId, EntityContext.LockMode lock) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO> results = null;
    try {
      session.currentTransaction().begin();
      session.setLockMode(getLock(lock));
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<InodeDTO> dobj =
              qb.createQueryDefinition(InodeDTO.class);
      HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred2);
      HopsQuery<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("parentIDParam", parentId);

      ArrayList<ProjectedINode> resultList = new ArrayList<>();
      results = query.getResultList();
      for (InodeDTO inode : results) {
        resultList.add(createProjectedINode(inode));
      }
      session.currentTransaction().commit();
      return resultList;
    }catch(StorageException e){
      session.currentTransaction().rollback();
      throw e;
    } finally {
      session.release(results);
    }
  }

  private ProjectedINode createProjectedINode(InodeDTO inode){
    return new ProjectedINode(inode.getId(),
              inode.getParentId(),
              inode.getName(),
              inode.getPartitionId(),
              NdbBoolean.convert(inode.getIsDir()),
              inode.getPermission(),
              inode.getUserID(),
              inode.getGroupID(),
              inode.getHeader(),
              inode.getSymlink()== null ? false : true,
              NdbBoolean.convert(inode.getQuotaEnabled()),
              NdbBoolean.convert(inode.getUnderConstruction()),
              NdbBoolean.convert(inode.getSubtreeLocked()),
              inode.getSubtreeLockOwner(),
              inode.getSize(),
              inode.getLogicalTime(),
              inode.getStoragePolicy(),
              inode.getNumAces(),
              inode.getNumUserXAttrs(),
              inode.getNumSysXAttrs());
  }
//  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockFTIS(
//      int parentId) throws StorageException {
//    final String query = String.format(
//        "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s " +
//            "WHERE %s=%d FOR UPDATE",
//        ID, NAME, PARENT_ID, PARTITION_ID, IS_DIR, PERMISSION, USER_ID, GROUP_ID, HEADER, SYMLINK,
//        QUOTA_ENABLED,
//        UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, SIZE, TABLE_NAME,
//        PARENT_ID, parentId);
//    ArrayList<ProjectedINode> resultList;
//    try {
//      Connection conn = mysqlConnector.obtainSession();
//      PreparedStatement s = conn.prepareStatement(query);
//      ResultSet result = s.executeQuery();
//      resultList = new ArrayList<ProjectedINode>();
//
//      while (result.next()) {
//        resultList.add(
//            new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
//                result.getString(NAME), result.getInt(PARTITION_ID),
//                result.getBoolean(IS_DIR), result.getShort(PERMISSION),
//                result.getInt(USER_ID), result.getInt(GROUP_ID),
//                result.getLong(HEADER),
//                result.getString(SYMLINK) == null ? false : true,
//                result.getBoolean(QUOTA_ENABLED),
//                result.getBoolean(UNDER_CONSTRUCTION),
//                result.getBoolean(SUBTREE_LOCKED),
//                result.getLong(SUBTREE_LOCK_OWNER),
//                result.getLong(SIZE)));
//      }
//    } catch (SQLException ex) {
//      throw HopsSQLExceptionHelper.wrap(ex);
//    } finally {
//      mysqlConnector.closeSession();
//    }
//    return resultList;
//  }


  @Override
  public List<INode> lockInodesUsingPkBatchTx(String[] names, long[] parentIds, long[] partitionIds,
                                              EntityContext.LockMode lock)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    session.currentTransaction().begin();
    session.setLockMode(getLock(lock));
    List<InodeDTO> dtos = new ArrayList<>();
    try {
      for (int i = 0; i < names.length; i++) {
        InodeDTO dto = session
                .newInstance(InodeDTO.class, new Object[]{partitionIds[i], parentIds[i], names[i]});
        dto.setId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      List<INode> inodeList = convert(dtos);
      session.currentTransaction().commit();
      return inodeList;
    }catch(StorageException e){
      session.currentTransaction().rollback();
      throw e;
    } finally {
      session.release(dtos);
    }
  }


  @Override
  public List<ProjectedINode> findInodesPPISTx(
          long parentId, long partitionId, EntityContext.LockMode lock) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<InodeDTO>  results = null;
    try {
      session.currentTransaction().begin();
      session.setLockMode(getLock(lock));
      HopsQueryBuilder qb = session.getQueryBuilder();
      HopsQueryDomainType<InodeDTO> dobj =
              qb.createQueryDefinition(InodeDTO.class);
      HopsPredicate pred1 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
      HopsPredicate pred2 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
      dobj.where(pred1.and(pred2));
      HopsQuery<InodeDTO> query = session.createQuery(dobj);
      query.setParameter("partitionIDParam", partitionId);
      query.setParameter("parentIDParam", parentId);

      ArrayList<ProjectedINode> resultList = new ArrayList<>();
      results = query.getResultList();
      for (InodeDTO inode : results) {
        resultList.add(createProjectedINode(inode));
      }
      session.currentTransaction().commit();
      return resultList;
    }catch(StorageException e){
      session.currentTransaction().rollback();
      throw e;
    } finally {
      session.release(results);
    }
  }
//  public List<ProjectedINode> findInodesForSubtreeOperationsWithWriteLockPPIS(
//      int parentId, int partitionId) throws StorageException {
//    final String query = String.format(
//        "SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s FROM %s " +
//            "WHERE %s=%d and %s=%d FOR UPDATE",
//        ID, NAME, PARENT_ID, PARTITION_ID, IS_DIR, PERMISSION, USER_ID, GROUP_ID, HEADER, SYMLINK,
//        QUOTA_ENABLED,
//        UNDER_CONSTRUCTION, SUBTREE_LOCKED, SUBTREE_LOCK_OWNER, SIZE, TABLE_NAME,
//        PARENT_ID, parentId, PARTITION_ID, partitionId);
//    ArrayList<ProjectedINode> resultList;
//    try {
//      Connection conn = mysqlConnector.obtainSession();
//      PreparedStatement s = conn.prepareStatement(query);
//      ResultSet result = s.executeQuery();
//      resultList = new ArrayList<ProjectedINode>();
//
//      while (result.next()) {
//        resultList.add(
//            new ProjectedINode(result.getInt(ID), result.getInt(PARENT_ID),
//                result.getString(NAME), result.getInt(PARTITION_ID),
//                result.getBoolean(IS_DIR), result.getShort(PERMISSION),
//                result.getInt(USER_ID), result.getInt(GROUP_ID),
//                result.getLong(HEADER),
//                result.getString(SYMLINK) == null ? false : true,
//                result.getBoolean(QUOTA_ENABLED),
//                result.getBoolean(UNDER_CONSTRUCTION),
//                result.getBoolean(SUBTREE_LOCKED),
//                result.getLong(SUBTREE_LOCK_OWNER),
//                result.getLong(SIZE)));
//      }
//    } catch (SQLException ex) {
//      throw HopsSQLExceptionHelper.wrap(ex);
//    } finally {
//      mysqlConnector.closeSession();
//    }
//    return resultList;
//  }

  @Override
  public INode findInodeByNameParentIdAndPartitionIdPK(String name, long parentId, long partitionId)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    Object[] pk = new Object[3];
    pk[0] = partitionId;
    pk[1] = parentId;
    pk[2] = name;

    InodeDTO result = session.find(InodeDTO.class, pk);
    if (result != null) {
      INode inode = convert(result);
      session.release(result);
      return inode;
    } else {
      return null;
    }
  }

  @Override
  public List<INode> getINodesPkBatched(String[] names, long[] parentIds, long[] partitionIds)
      throws StorageException {
    HopsSession session = connector.obtainSession();

    List<InodeDTO> dtos = new ArrayList<>();
    try {
      for (int i = 0; i < names.length; i++) {
        InodeDTO dto = session
                .newInstance(InodeDTO.class, new Object[]{partitionIds[i], parentIds[i], names[i]});
        dto.setId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      List<INode> inodeList = convert(dtos);
      return inodeList;
    } finally {
      session.release(dtos);
    }
  }

  private boolean isRoot(INode inode){
    return inode.getName().equals("") && inode.getParentId() == 0 && inode
        .getId() == 1;
  }

  @Override
  public List<INodeIdentifier> getAllINodeFiles(long startId, long endId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred = dobj.get("isDir").equal(dobj.param("isDirParam"));
    HopsPredicate pred2 =
        dobj.get("id").between(dobj.param("startId"), dobj.param("endId"));
    dobj.where(pred.not().and(pred2));
    HopsQuery<InodeDTO> query = session.createQuery(dobj);
    query.setParameter("isDirParam", NdbBoolean.convert(true));
    //startId is inclusive and endId exclusive
    query.setParameter("startId", startId);
    query.setParameter("endId", endId - 1);
    List<InodeDTO> dtos = query.getResultList();
    List<INodeIdentifier> res = new ArrayList<>();
    for (InodeDTO dto : dtos) {
      res.add(
          new INodeIdentifier(dto.getId(), dto.getParentId(), dto.getName(), dto.getPartitionId()));
    }
    session.release(dtos);
    return res;
  }
  
  
  @Override
  public boolean haveFilesWithIdsBetween(long startId, long endId)
      throws StorageException {
    return MySQLQueryHelper.exists(TABLE_NAME, String
        .format("%s<>0 and %s " + "between %d and %d", HEADER, ID, startId,
            (endId - 1)));
  }
  
  @Override
  public boolean haveFilesWithIdsGreaterThan(long id) throws StorageException {
    return MySQLQueryHelper.exists(TABLE_NAME,
        String.format("%s<>0 and " + "%s>%d", HEADER, ID, id));
  }
  
  @Override
  public long getMinFileId() throws StorageException {
    return MySQLQueryHelper
        .minLong(TABLE_NAME, ID, String.format("%s<>0", HEADER));
  }

  @Override
  public long getMaxFileId() throws StorageException {
    return MySQLQueryHelper
        .maxLong(TABLE_NAME, ID, String.format("%s<>0", HEADER));
  }

  @Override
  public int countAllFiles() throws StorageException {
    return MySQLQueryHelper
        .countWithCriterion(TABLE_NAME, String.format("%s<>0", HEADER));
  }

  @Override
  public void deleteInode(String inodeName) throws StorageException { // only for testing
    String query = "delete from "+TablesDef.INodeTableDef.TABLE_NAME+" where "+
            TablesDef.INodeTableDef.NAME +" = \""+inodeName+"\"";
    MySQLQueryHelper.execute(query);
  }
  
  @Override
  public List<INode> allINodes() throws StorageException { // only for testing
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQuery<InodeDTO> query =
        session.createQuery(qb.createQueryDefinition(InodeDTO.class));
    List<InodeDTO> dtos = query.getResultList();
    List<INode> list = convert(dtos);
    session.release(dtos);
    return list;
  }
  
  @Override
  public boolean hasChildren(long parentId, boolean areChildRandomlyPartitioned) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<InodeDTO> dobj =
        qb.createQueryDefinition(InodeDTO.class);
    HopsPredicate pred1 = dobj.get("parentId").equal(dobj.param("parentIDParam"));
    HopsPredicate pred2 = dobj.get("partitionId").equal(dobj.param("partitionIDParam"));
    if(areChildRandomlyPartitioned){
      dobj.where(pred1);
    }else{
      dobj.where(pred2.and(pred1));
    }
    HopsQuery<InodeDTO> query = session.createQuery(dobj);

    query.setParameter("parentIDParam", parentId);
    if(!areChildRandomlyPartitioned){
      query.setParameter("partitionIDParam", parentId);
    }
    query.setLimits(0, 1);

    List<InodeDTO> results = query.getResultList();
    if(results.isEmpty()){
      return false;
    }else{
      session.release(results);
      return true;
    }
  }

  @Override
  public void updateLogicalTime(Collection<INodeMetadataLogEntry> logEntries)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    for(INodeMetadataLogEntry logEntry : logEntries){
      InodeDTO inodeDTO = createPersistable(session, logEntry);
      session.savePersistent(inodeDTO);
      session.release(inodeDTO);
    }
  }

  @Override
  public int countSubtreeLockedInodes() throws StorageException {
    String query = TablesDef.INodeTableDef.SUBTREE_LOCKED +" = 1";
    return MySQLQueryHelper.countWithCriterion(TablesDef.INodeTableDef.TABLE_NAME, query);
  }

  private InodeDTO createPersistable(HopsSession session, INodeMetadataLogEntry
      logEntry) throws StorageException {
    InodeDTO inodeDTO = session.newInstance(InodeDTO.class);
    inodeDTO.setPartitionId(logEntry.getPartitionId());
    inodeDTO.setParentId(logEntry.getParentId());
    inodeDTO.setName(logEntry.getName());
    inodeDTO.setLogicalTime(logEntry.getLogicalTime());
    return inodeDTO;
  }

  @Override
  public long getMaxId() throws StorageException{
    return MySQLQueryHelper.maxLong(TABLE_NAME, ID);
  }
  
  private List<INode> convert(List<InodeDTO> list) throws StorageException {
    List<INode> inodes = new ArrayList<>();
    for (InodeDTO persistable : list) {
      if (persistable.getId() != NOT_FOUND_ROW) {
        inodes.add(convert(persistable));
      }
    }
    return inodes;
  }


  protected static INode convert(InodeDTO persistable) {
    INode node = new INode(persistable.getId(), persistable.getName(),
        persistable.getParentId(), persistable.getPartitionId(),
        NdbBoolean.convert(persistable.getIsDir()),
        NdbBoolean.convert(persistable.getQuotaEnabled()),
        persistable.getModificationTime(), persistable.getATime(),
        persistable.getUserID(), persistable.getGroupID(),
        persistable.getPermission(), NdbBoolean.convert(persistable.getUnderConstruction()),
        persistable.getClientName(), persistable.getClientMachine(),
        persistable.getGenerationStamp(),
        persistable.getHeader(), persistable.getSymlink(),
        NdbBoolean.convert(persistable.getSubtreeLocked()),
        persistable.getSubtreeLockOwner(),
        persistable.getMetaEnabled(),
        persistable.getSize(), NdbBoolean.convert(persistable
        .getFileStoredInDd()), persistable.getLogicalTime(),
        persistable.getStoragePolicy(), persistable.getChildrenNum(),
        persistable.getNumAces(), persistable.getNumUserXAttrs(),
        persistable.getNumSysXAttrs());

    return node;
  }

  protected static void createPersistable(INode inode, InodeDTO persistable) {
    persistable.setId(inode.getId());
    persistable.setName(inode.getName());
    persistable.setParentId(inode.getParentId());
    persistable.setQuotaEnabled(NdbBoolean.convert(inode.isDirWithQuota()));
    persistable.setModificationTime(inode.getModificationTime());
    persistable.setATime(inode.getAccessTime());
    persistable.setUserID(inode.getUserID());
    persistable.setGroupID(inode.getGroupID());
    persistable.setPermission(inode.getPermission());
    persistable.setUnderConstruction(NdbBoolean.convert(inode.isUnderConstruction()));
    persistable.setClientName(inode.getClientName());
    persistable.setClientMachine(inode.getClientMachine());
    persistable.setGenerationStamp(inode.getGenerationStamp());
    persistable.setHeader(inode.getHeader());
    persistable.setSymlink(inode.getSymlink());
    persistable.setSubtreeLocked(NdbBoolean.convert(inode.isSubtreeLocked()));
    persistable.setSubtreeLockOwner(inode.getSubtreeLockOwner());
    persistable.setSize(inode.getFileSize());
    persistable.setMetaEnabled(inode.getMetaStatus().getVal());
    persistable.setFileStoredInDd(NdbBoolean.convert(inode.isFileStoredInDB()));
    persistable.setIsDir(NdbBoolean.convert(inode.isDirectory()));
    persistable.setPartitionId(inode.getPartitionId());
    persistable.setLogicalTime(inode.getLogicalTime());
    persistable.setStoragePolicy(inode.getStoragePolicyID());
    persistable.setChildrenNum(inode.getChildrenNum());
    persistable.setNumAces(inode.getNumAces());
    persistable.setNumUserXAttrs(inode.getNumUserXAttrs());
    persistable.setNumSysXAttrs(inode.getNumSysXAttrs());
  }

  private void explain(HopsQuery<InodeDTO> query) {
//    Map<String, Object> map = null;
//    try {
//      map = query.explain();
//      System.out.println("Explain");
//      System.out.println("keys " + Arrays.toString(map.keySet().toArray()));
//      System.out.println("values " + Arrays.toString(map.values().toArray()));
//
//    } catch (StorageException e) {
//      e.printStackTrace();
//    }
  }

   com.mysql.clusterj.LockMode getLock(EntityContext.LockMode lock){
    switch(lock){
      case WRITE_LOCK:
        return LockMode.EXCLUSIVE;
      case READ_LOCK:
        return LockMode.SHARED;
      case READ_COMMITTED:
        return LockMode.READ_COMMITTED;
      default:
        throw new UnsupportedOperationException("Lock Type is not supported");
    }
   }

}
