package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.PersistenceCapable;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME5;

@PersistenceCapable(table = TABLE_NAME5)
public interface WriteAcknowledgementDeployment5 extends WriteAcknowledgementDTO {
}
