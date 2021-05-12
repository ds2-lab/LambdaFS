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
import io.hops.metadata.hdfs.dal.UserDataAccess;
import io.hops.metadata.hdfs.entity.User;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.List;


public class UserClusterj implements TablesDef.UsersTableDef, UserDataAccess<User>{
  
  @PersistenceCapable(table = TABLE_NAME)
  public interface UserDTO {

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
  public User getUser(final int userId) throws StorageException {
    HopsSession session = connector.obtainSession();
    UserDTO dto = session.find(UserDTO.class, userId);
    User user = null;
    if(dto != null) {
      user = new User(dto.getId(), dto.getName());
      session.release(dto);
    }
    return user;
  }

  @Override
  public User getUser(final String userName) throws StorageException {
    HopsSession session = connector.obtainSession();
    return getUser(session, userName);
  }

  @Override
  public User addUser(String userName) throws StorageException{
    HopsSession session = connector.obtainSession();
    User user = getUser(session, userName);
    if(user == null){
      addUser(session, userName);
      session.flush();
      user = getUser(session, userName);
    }
    return user;
  }

  @Override
  public void removeUser(int userId) throws StorageException {
    HopsSession session = connector.obtainSession();
    UserDTO dto = null;
    try {
      dto = session.newInstance(UserDTO.class, userId);
      session.deletePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  private void addUser(HopsSession session, final String userName)
      throws StorageException {
    UserDTO dto = null;
    try {
      dto = session.newInstance(UserDTO.class);
      dto.setName(userName);
      session.makePersistent(dto);
    }finally {
      session.release(dto);
    }
  }

  private User getUser(HopsSession session, final String userName) throws
      StorageException {
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<UserDTO> dobj =  qb.createQueryDefinition
        (UserDTO.class);
    dobj.where(dobj.get("name").equal(dobj.param("param")));
    HopsQuery<UserDTO> query = session.createQuery(dobj);
    query.setParameter("param", userName);
    List<UserDTO> results = query.getResultList();
    User user = null;
    if(results.size() == 1) {
      user = new User(results.get(0).getId(), results.get(0).getName());
    }
    session.release(results);
    return user;
  }
}
