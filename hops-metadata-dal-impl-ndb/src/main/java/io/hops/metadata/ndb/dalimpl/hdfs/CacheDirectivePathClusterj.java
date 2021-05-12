/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsPredicate;
import io.hops.metadata.ndb.wrapper.HopsQuery;
import io.hops.metadata.ndb.wrapper.HopsQueryBuilder;
import io.hops.metadata.ndb.wrapper.HopsQueryDomainType;
import io.hops.metadata.ndb.wrapper.HopsSession;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 * @author gautier
 */
public class CacheDirectivePathClusterj implements TablesDef.CacheDirectivePathTableDef {

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private int chunkSize = 250;

  @PersistenceCapable(table = TABLE_NAME)
  public interface CacheDirectivePathDTO {

    @PrimaryKey
    @Column(name = ID)
    long getId();

    void setId(long id);

    @PrimaryKey
    @Column(name = INDEX)
    short getIndex();

    void setIndex(short replication);

    @Column(name = VALUE)
    String getValue();

    void setValue(String value);
  }

  public String find(final long id) throws StorageException {
    HopsSession session = connector.obtainSession();

    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<CacheDirectivePathDTO> dobj = qb.createQueryDefinition(CacheDirectivePathDTO.class);
    HopsPredicate pred1 = dobj.get("id").equal(dobj.param("id"));
    dobj.where(pred1);
    HopsQuery<CacheDirectivePathDTO> query = session.createQuery(dobj);
    query.setParameter("id", id);
    List<CacheDirectivePathDTO> results = null;

    try {
      results = query.getResultList();
      if (results == null) {
        return null;
      } else {
        return convert(results);
      }
    } finally {
      session.release(results);
    }
  }

  public void add(Map<Long, String> modified) throws StorageException {

    List<CacheDirectivePathDTO> changes = new ArrayList<>();
    HopsSession session = connector.obtainSession();

    for (long id : modified.keySet()) {
      changes.addAll(createPersistable(id, modified.get(id), session));
    }
    session.savePersistentAll(changes);

    session.release(changes);

  }

  private String convert(List<CacheDirectivePathDTO> pathParts) {
    String[] pathArray = new String[pathParts.size()];
    for (CacheDirectivePathDTO part : pathParts) {
      pathArray[part.getIndex()] = part.getValue();
    }
    return String.join("", pathArray);
  }

  private Collection<CacheDirectivePathDTO> createPersistable(long id, String path, HopsSession session) throws
      StorageException {
    int nbChunks = (int) Math.ceil((float)path.length() / chunkSize);
    List<CacheDirectivePathDTO> pathParts = new ArrayList<>(nbChunks);
    for (short i = 0; i < nbChunks; i++) {
      CacheDirectivePathDTO newInstance = session.newInstance(CacheDirectivePathDTO.class);
      newInstance.setId(id);
      newInstance.setIndex(i);
      newInstance.setValue(path.substring(i * chunkSize, Math.min((i + 1) * chunkSize, path.length())));
      pathParts.add(newInstance);
    }
    return pathParts;
  }
}
