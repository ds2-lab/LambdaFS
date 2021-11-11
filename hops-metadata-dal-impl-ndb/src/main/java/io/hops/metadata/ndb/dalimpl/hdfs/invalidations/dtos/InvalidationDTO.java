package io.hops.metadata.ndb.dalimpl.hdfs.invalidations.dtos;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.metadata.hdfs.TablesDef;

public interface InvalidationDTO extends TablesDef.InvalidationTablesDef {
    @PrimaryKey
    @Column(name = INODE_ID)
    public int getINodeId();
    public void setINodeId(int inodeId);

    @Column(name = PARENT_ID)
    public int getParentId();
    public void setParentId(int parentId);

    @PrimaryKey
    @Column(name = LEADER_ID)
    public long getLeaderId();
    public void setLeaderId(long leaderId);

    @Column(name = TX_START)
    public long getTxStart();
    public void setTxStart(long txStart);

    @PrimaryKey
    @Column(name = OPERATION_ID)
    public long getOperationId();
    public void setOperationId(long operationId);


}
