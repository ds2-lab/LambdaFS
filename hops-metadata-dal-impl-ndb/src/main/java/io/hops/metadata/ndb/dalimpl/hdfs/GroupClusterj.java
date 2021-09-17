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


import com.google.common.collect.Lists;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.GroupDataAccess;
import io.hops.metadata.hdfs.entity.Group;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

public class GroupClusterj implements TablesDef.GroupsTableDef, GroupDataAccess<Group>{

  @PersistenceCapable(table = TABLE_NAME)
  public interface GroupDTO {

    @PrimaryKey
    @Column(name = ID)
    int getId();

    void setId(int id);

    @Column(name = NAME)
    String getName();

    void setName(String name);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public Group getGroup(final int groupId) throws StorageException {
    HopsSession session = connector.obtainSession();
    GroupDTO dto = session.find(GroupDTO.class, groupId);
    Group group = null;
    if(dto != null) {
      group = new Group(dto.getId(), dto.getName());
      session.release(dto);
    }
    return group;
  }

  @Override
  public Group getGroup(final String groupName) throws StorageException {
    HopsSession session = connector.obtainSession();
    return getGroup(session, groupName);
  }


  @Override
  public Group addGroup(String groupName) throws StorageException {
    HopsSession session = connector.obtainSession();
    Group group = getGroup(session, groupName);
    if(group == null){
      addGroup(session, groupName);
      session.flush();
      group = getGroup(session, groupName);
    }
    return group;
  }

  @Override
  public void removeGroup(int groupId) throws StorageException {
    HopsSession session = connector.obtainSession();
    GroupDTO dto = null;
    try {
      dto = session.newInstance(GroupDTO.class, groupId);
      session.deletePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  static List<Group> convert(HopsSession session, Collection<GroupDTO>
      dtos)
      throws StorageException {
    List<Group> groups = Lists.newArrayListWithExpectedSize(dtos.size());
    for(GroupDTO dto : dtos){
      groups.add(new Group(dto.getId(), dto.getName()));
    }
    return groups;
  }

  private void addGroup(HopsSession session, String groupName)
      throws StorageException {
    GroupDTO dto = null;
    try {
      dto = session.newInstance(GroupDTO.class);
      dto.setName(groupName);
      session.makePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  private Group getGroup(HopsSession session, final String groupName) throws
      StorageException {
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<GroupDTO> dobj =  qb.createQueryDefinition
        (GroupDTO.class);
    dobj.where(dobj.get("name").equal(dobj.param("param")));
    HopsQuery<GroupDTO> query = session.createQuery(dobj);
    query.setParameter("param", groupName);
    List<GroupDTO> results = query.getResultList();
    Group group = null;
    if(results.size() == 1){
      group = new Group(results.get(0).getId(), results.get(0).getName());
    }
    session.release(results);
    return group;
  }
}
