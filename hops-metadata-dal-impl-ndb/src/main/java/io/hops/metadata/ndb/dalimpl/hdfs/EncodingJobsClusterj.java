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
import io.hops.metadata.hdfs.dal.EncodingJobsDataAccess;
import io.hops.metadata.hdfs.entity.EncodingJob;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class EncodingJobsClusterj implements TablesDef.EncodingJobsTableDef,
    EncodingJobsDataAccess<EncodingJob> {
  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @PersistenceCapable(table = TABLE_NAME)
  public interface EncodingJobDto {

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

    @Column(name = JOB_DIR)
    String getJobDir();

    void setJobDir(String jobDir);
  }

  @Override
  public void add(EncodingJob encodingJob) throws StorageException {
    HopsSession session = connector.obtainSession();
    EncodingJobDto dto = null;
    try {
      dto = createPersistable(encodingJob);
      session.makePersistent(dto);
    } finally {
      session.release(dto);
    }
  }

  @Override
  public void delete(EncodingJob encodingJob) throws StorageException {
    HopsSession session = connector.obtainSession();
    EncodingJobDto dto = null;
    try {
      dto = createPersistable(encodingJob);
      session.deletePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  @Override
  public Collection<EncodingJob> findAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<EncodingJobDto> dobj =
        qb.createQueryDefinition(EncodingJobDto.class);
    HopsQuery<EncodingJobDto> query = session.createQuery(dobj);
    
    List<EncodingJobDto> dtos = query.getResultList();
    Collection<EncodingJob> ivl = createList(dtos);
    session.release(dtos);
    return ivl;
  }

  private EncodingJobDto createPersistable(EncodingJob encodingJob)
      throws StorageException {
    HopsSession session = connector.obtainSession();
    EncodingJobDto dto = session.newInstance(EncodingJobDto.class);
    dto.setJtidentifier(encodingJob.getJtIdentifier());
    dto.setJobId(encodingJob.getJobId());
    dto.setPath(encodingJob.getPath());
    dto.setJobDir(encodingJob.getJobDir());
    return dto;
  }

  private EncodingJob create(EncodingJobDto dto) {
    EncodingJob job = new EncodingJob();
    job.setJtIdentifier(dto.getJtidentifier());
    job.setJobId(dto.getJobId());
    job.setPath(dto.getPath());
    job.setJobDir(dto.getJobDir());
    return job;
  }

  private List<EncodingJob> createList(List<EncodingJobDto> dtos) {
    ArrayList<EncodingJob> list =
        new ArrayList<>(dtos.size());
    for (EncodingJobDto dto : dtos) {
      list.add(create(dto));
    }
    return list;
  }
}
