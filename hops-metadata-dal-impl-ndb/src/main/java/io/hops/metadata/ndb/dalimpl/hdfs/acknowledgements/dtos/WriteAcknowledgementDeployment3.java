package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME3;

@PersistenceCapable(table = TABLE_NAME3)
public interface WriteAcknowledgementDeployment3 extends WriteAcknowledgementDTO {
}
