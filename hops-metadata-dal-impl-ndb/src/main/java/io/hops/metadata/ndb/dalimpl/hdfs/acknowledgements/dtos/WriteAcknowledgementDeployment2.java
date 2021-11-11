package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.metadata.hdfs.TablesDef;

import static io.hops.metadata.hdfs.TablesDef.WriteAcknowledgementsTableDef.TABLE_NAME0;

@PersistenceCapable(table = TABLE_NAME2)
public interface WriteAcknowledgementDeployment2 extends WriteAcknowledgementDTO {

}