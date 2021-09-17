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

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import static io.hops.metadata.hdfs.TablesDef.DirectoryWithQuotaFeatureTableDef.TYPESPACE_QUOTA_DB;
import static io.hops.metadata.hdfs.TablesDef.DirectoryWithQuotaFeatureTableDef.TYPESPACE_USED_DB;
import io.hops.metadata.hdfs.entity.DirectoryWithQuotaFeature;
import io.hops.metadata.hdfs.entity.INodeCandidatePrimaryKey;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import io.hops.metadata.hdfs.dal.DirectoryWithQuotaFeatureDataAccess;
import io.hops.metadata.hdfs.entity.QuotaUpdate;
import java.util.HashMap;
import java.util.Map;

public class DirectoryWithQuotaFeatureClusterj implements
    TablesDef.DirectoryWithQuotaFeatureTableDef,
    DirectoryWithQuotaFeatureDataAccess<DirectoryWithQuotaFeature> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface INodeAttributesDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @Column(name = NSQUOTA)
    long getNSQuota();

    void setNSQuota(long nsquota);

    @Column(name = SSQUOTA)
    long getSSQuota();

    void setSSQuota(long dsquota);

    @Column(name = NSCOUNT)
    long getNSCount();

    void setNSCount(long nscount);

    @Column(name = STORAGESPACE)
    long getStorageSpace();

    void setStorageSpace(long storageSpace);
    
    @Column(name = TYPESPACE_QUOTA_DISK)
    long getTypeSpaceQuotaDisk();
    
    void setTypeSpaceQuotaDisk(long quota);
    
    @Column(name = TYPESPACE_QUOTA_SSD)
    long getTypeSpaceQuotaSSD();
    
    void setTypeSpaceQuotaSSD(long quota);
    
    @Column(name = TYPESPACE_QUOTA_RAID5)
    long getTypeSpaceQuotaRaid5();
    
    void setTypeSpaceQuotaRaid5(long quota);
    
    @Column(name = TYPESPACE_QUOTA_ARCHIVE)
    long getTypeSpaceQuotaArchive();
    
    void setTypeSpaceQuotaArchive(long quota);

    @Column(name = TYPESPACE_QUOTA_DB)
    long getTypeSpaceQuotaDb();

    void setTypeSpaceQuotaDb(long quota);
    
    @Column(name = TYPESPACE_QUOTA_PROVIDED)
    long getTypeSpaceQuotaProvided();

    void setTypeSpaceQuotaProvided(long quota);

    @Column(name = TYPESPACE_USED_DISK)
    long getTypeSpaceUsedDisk();
    
    void setTypeSpaceUsedDisk(long used);
    
    @Column(name = TYPESPACE_USED_SSD)
    long getTypeSpaceUsedSSD();
    
    void setTypeSpaceUsedSSD(long used);
    
    @Column(name = TYPESPACE_USED_RAID5)
    long getTypeSpaceUsedRaid5();
    
    void setTypeSpaceUsedRaid5(long used);
    
    @Column(name = TYPESPACE_USED_ARCHIVE)
    long getTypeSpaceUsedArchive();
    
    void setTypeSpaceUsedArchive(long used);

    @Column(name = TYPESPACE_USED_DB)
    long getTypeSpaceUsedDb();

    void setTypeSpaceUsedDb(long used);
    
    @Column(name = TYPESPACE_USED_PROVIDED)
    long getTypeSpaceUsedProvided();

    void setTypeSpaceUsedProvided(long used);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public DirectoryWithQuotaFeature findAttributesByPk(Long inodeId)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    INodeAttributesDTO dto = session.find(INodeAttributesDTO.class, inodeId);
    DirectoryWithQuotaFeature iNodeAttributes =  null;
    if(dto != null){
        iNodeAttributes = makeINodeAttributes(dto);
        session.release(dto);
    }
    return iNodeAttributes;
  }
  
  @Override
  public Collection<DirectoryWithQuotaFeature> findAttributesByPkList(
      List<INodeCandidatePrimaryKey> inodePks) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DirectoryWithQuotaFeature> inodeAttributesBatchResponse =
        new ArrayList<>();
    List<INodeAttributesDTO> inodeAttributesBatchRequest =
        new ArrayList<>();

    try {
      for (INodeCandidatePrimaryKey pk : inodePks) {
        INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
        dto.setId(pk.getInodeId());
        inodeAttributesBatchRequest.add(dto);
        session.load(dto);
      }

      session.flush();
  
      for (INodeAttributesDTO anInodeAttributesBatchRequest : inodeAttributesBatchRequest) {
        inodeAttributesBatchResponse
            .add(makeINodeAttributes(anInodeAttributesBatchRequest));
      }

      return inodeAttributesBatchResponse;
    } finally {
      session.release(inodeAttributesBatchRequest);
    }
  }

  @Override
  public void prepare(Collection<DirectoryWithQuotaFeature> modified,
      Collection<DirectoryWithQuotaFeature> removed) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<INodeAttributesDTO> changes = new ArrayList<>();
    List<INodeAttributesDTO> deletions = new ArrayList<>();
    try {
      if (removed != null) {
        for (DirectoryWithQuotaFeature attr : removed) {
          INodeAttributesDTO persistable =
                  session.newInstance(INodeAttributesDTO.class, attr.getInodeId());
          deletions.add(persistable);
        }
      }
      if (modified != null) {
        for (DirectoryWithQuotaFeature attr : modified) {
          INodeAttributesDTO persistable = createPersistable(attr, session);
          changes.add(persistable);
        }
      }
      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private INodeAttributesDTO createPersistable(DirectoryWithQuotaFeature dir,
      HopsSession session) throws StorageException {
    INodeAttributesDTO dto = session.newInstance(INodeAttributesDTO.class);
    dto.setId(dir.getInodeId());
    dto.setNSQuota(dir.getNsQuota());
    dto.setNSCount(dir.getNsUsed());
    dto.setSSQuota(dir.getSSQuota());
    dto.setStorageSpace(dir.getSSUsed());

    dto.setTypeSpaceQuotaDisk(dir.getTypeQuota().get(QuotaUpdate.StorageType.DISK));
    dto.setTypeSpaceQuotaSSD(dir.getTypeQuota().get(QuotaUpdate.StorageType.SSD));
    dto.setTypeSpaceQuotaRaid5(dir.getTypeQuota().get(QuotaUpdate.StorageType.RAID5));
    dto.setTypeSpaceQuotaArchive(dir.getTypeQuota().get(QuotaUpdate.StorageType.ARCHIVE));
    dto.setTypeSpaceQuotaDb(dir.getTypeQuota().get(QuotaUpdate.StorageType.DB));
    dto.setTypeSpaceQuotaProvided(dir.getTypeQuota().get(QuotaUpdate.StorageType.PROVIDED));

    dto.setTypeSpaceUsedDisk(dir.getTypeUsed().get(QuotaUpdate.StorageType.DISK));
    dto.setTypeSpaceUsedSSD(dir.getTypeUsed().get(QuotaUpdate.StorageType.SSD));
    dto.setTypeSpaceUsedRaid5(dir.getTypeUsed().get(QuotaUpdate.StorageType.RAID5));
    dto.setTypeSpaceUsedArchive(dir.getTypeUsed().get(QuotaUpdate.StorageType.ARCHIVE));
    dto.setTypeSpaceUsedDb(dir.getTypeUsed().get(QuotaUpdate.StorageType.DB));
    dto.setTypeSpaceUsedProvided(dir.getTypeUsed().get(QuotaUpdate.StorageType.PROVIDED));

    return dto;
  }

  private DirectoryWithQuotaFeature makeINodeAttributes(INodeAttributesDTO dto) {
    if (dto == null) {
      return null;
    }
    Map<QuotaUpdate.StorageType, Long> typeQuota = new HashMap<>();
    typeQuota.put(QuotaUpdate.StorageType.DISK, dto.getTypeSpaceQuotaDisk());
    typeQuota.put(QuotaUpdate.StorageType.SSD, dto.getTypeSpaceQuotaSSD());
    typeQuota.put(QuotaUpdate.StorageType.RAID5, dto.getTypeSpaceQuotaRaid5());
    typeQuota.put(QuotaUpdate.StorageType.ARCHIVE, dto.getTypeSpaceQuotaArchive());
    typeQuota.put(QuotaUpdate.StorageType.DB, dto.getTypeSpaceQuotaDb());
    typeQuota.put(QuotaUpdate.StorageType.PROVIDED, dto.getTypeSpaceQuotaProvided());

    Map<QuotaUpdate.StorageType, Long> typeUsed = new HashMap<>();
    typeUsed.put(QuotaUpdate.StorageType.DISK, dto.getTypeSpaceUsedDisk());
    typeUsed.put(QuotaUpdate.StorageType.SSD, dto.getTypeSpaceUsedSSD());
    typeUsed.put(QuotaUpdate.StorageType.RAID5, dto.getTypeSpaceUsedRaid5());
    typeUsed.put(QuotaUpdate.StorageType.ARCHIVE, dto.getTypeSpaceUsedArchive());
    typeUsed.put(QuotaUpdate.StorageType.DB, dto.getTypeSpaceUsedDb());
    typeUsed.put(QuotaUpdate.StorageType.PROVIDED, dto.getTypeSpaceUsedProvided());

    DirectoryWithQuotaFeature dir =
        new DirectoryWithQuotaFeature(dto.getId(), dto.getNSQuota(), dto.getNSCount(),
            dto.getSSQuota(), dto.getStorageSpace(), typeQuota, typeUsed);
    return dir;
  }
}