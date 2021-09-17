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
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.RepairJobsDataAccess;
import io.hops.metadata.hdfs.entity.RepairJob;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class RepairJobsClusterj implements TablesDef.RepairJobsTableDef,
    RepairJobsDataAccess<RepairJob> {
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface RepairJobDto {

    @PrimaryKey
    @Column(name = JT_IDENTIFIER)
    String getJtidentifier();

    void setJtidentifier(String jtidentifier);

    @PrimaryKey
    @Column(name = JOB_ID)
    int getJobId();

    void setJobId(int jobId);

    @Column(name = PATH)
    String getPath();

    void setPath(String path);

    @Column(name = IN_DIR)
    String getInDir();

    void setInDir(String inDir);

    @Column(name = OUT_DIR)
    String getOutDir();

    void setOutDir(String outDir);
  }

  @Override
  public void add(RepairJob repairJob) throws StorageException {
    HopsSession session = connector.obtainSession();
    RepairJobDto dto = null;
    try {
      dto = createPersistable(repairJob);
      session.makePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  @Override
  public void delete(RepairJob encodingJob) throws StorageException {
    HopsSession session = connector.obtainSession();
    RepairJobDto dto = null;
    try {
      dto = createPersistable(encodingJob);
      session.deletePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  @Override
  public Collection<RepairJob> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<RepairJobDto> dobj =
        qb.createQueryDefinition(RepairJobDto.class);
    HopsQuery<RepairJobDto> query = session.createQuery(dobj);
    return convertAndRelease(session, query.getResultList());
  }

  private RepairJobDto createPersistable(RepairJob repairJob)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    RepairJobDto dto = session.newInstance(RepairJobDto.class);
    dto.setJtidentifier(repairJob.getJtIdentifier());
    dto.setJobId(repairJob.getJobId());
    dto.setPath(repairJob.getPath());
    dto.setInDir(repairJob.getInDir());
    dto.setOutDir(repairJob.getOutDir());
    return dto;
  }

  private RepairJob convertAndRelease(HopsSession session, RepairJobDto dto)
      throws StorageException {
    RepairJob job = new RepairJob();
    job.setJtIdentifier(dto.getJtidentifier());
    job.setJobId(dto.getJobId());
    job.setPath(dto.getPath());
    job.setInDir(dto.getInDir());
    job.setOutDir(dto.getOutDir());
    session.release(dto);
    return job;
  }

  private List<RepairJob> convertAndRelease(HopsSession session,
      List<RepairJobDto> dtos) throws StorageException {
    ArrayList<RepairJob> list =
        new ArrayList<>(dtos.size());
    for (RepairJobDto dto : dtos) {
      list.add(convertAndRelease(session, dto));
    }
    return list;
  }
}
