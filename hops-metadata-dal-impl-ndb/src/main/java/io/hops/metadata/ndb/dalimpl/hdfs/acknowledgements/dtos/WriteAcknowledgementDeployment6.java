package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME6;

@PersistenceCapable(table = TABLE_NAME6)
public interface WriteAcknowledgementDeployment6 extends WriteAcknowledgementDTO {
}
