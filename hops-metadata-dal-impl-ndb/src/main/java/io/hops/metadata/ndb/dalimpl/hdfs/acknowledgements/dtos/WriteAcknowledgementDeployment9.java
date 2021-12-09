package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME9;

@PersistenceCapable(table = TABLE_NAME9)
public interface WriteAcknowledgementDeployment9 extends WriteAcknowledgementDTO {
}
