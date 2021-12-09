package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME4;

@PersistenceCapable(table = TABLE_NAME4)
public interface WriteAcknowledgementDeployment4 extends WriteAcknowledgementDTO {
}
