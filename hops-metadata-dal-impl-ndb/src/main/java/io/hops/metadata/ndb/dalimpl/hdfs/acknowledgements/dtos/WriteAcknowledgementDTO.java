package io.hops.metadata.ndb.dalimpl.hdfs.acknowledgements.dtos;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.metadata.hdfs.TablesDef;

public interface WriteAcknowledgementDTO extends TablesDef.WriteAcknowledgementsTableDef  {
    @PrimaryKey
    @Column(name = NAME_NODE_ID)
    public long getNameNodeId();
    public void setNameNodeId(long nameNodeId);

    @Column(name = DEPLOYMENT_NUMBER)
    public int getDeploymentNumber();
    public void setDeploymentNumber(int deploymentNumber);

    @Column(name = ACKNOWLEDGED)
    public byte getAcknowledged();
    public void setAcknowledged(byte acknowledged);

    @PrimaryKey
    @Column(name = OPERATION_ID)
    public long getOperationId();
    public void setOperationId(long operationId);

    @Column(name = TIMESTAMP)
    public long getTimestamp();
    public void setTimestamp(long timestamp);
}
