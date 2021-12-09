package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME8;

@PersistenceCapable(table = TABLE_NAME8)
public interface WriteAcknowledgementDeployment8 extends WriteAcknowledgementDTO {
}
