package io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;
import io.hops.metadata.hdfs.TablesDef;

@PersistenceCapable(table = TablesDef.InvalidationTablesDef.TABLE_NAME5)
public interface InvalidationDeployment5 extends InvalidationDTO {
}