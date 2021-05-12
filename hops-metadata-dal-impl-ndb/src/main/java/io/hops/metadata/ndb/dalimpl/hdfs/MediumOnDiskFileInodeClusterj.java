package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.MediumOnDiskInodeDataAccess;
import io.hops.metadata.hdfs.entity.FileInodeData;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

/**
 * Created by salman on 3/10/16.
 */
public class MediumOnDiskFileInodeClusterj
        implements TablesDef.FileInodeMediumlDiskData, MediumOnDiskInodeDataAccess<FileInodeData> {
  static final Logger LOG = Logger.getLogger(MediumOnDiskFileInodeClusterj.class);
  private ClusterjConnector connector = ClusterjConnector.getInstance();


  @PersistenceCapable(table = TABLE_NAME)
  public interface FileInodeDataDTO {

    @PrimaryKey
    @Column(name = ID)
    long getInodeId();

    void setInodeId(long inodeId);

    @Column(name = DATA)
    byte[] getData();

    void setData(byte[] data);
  }


  @Override
  public void add(FileInodeData fileInodeData) throws StorageException {
    if(fileInodeData.getDBFileStorageType() != FileInodeData.Type.OnDiskFile){
      throw new IllegalArgumentException("Expecting on disk file object. Got: "+fileInodeData.getDBFileStorageType());
    }

    final HopsSession session = connector.obtainSession();
    FileInodeDataDTO dto = session.newInstance(MediumOnDiskFileInodeClusterj.FileInodeDataDTO.class);
    dto.setInodeId(fileInodeData.getInodeId());
    dto.setData(fileInodeData.getInodeData());
    session.savePersistent(dto);
    session.release(dto);
  }

  @Override
  public void delete(FileInodeData fileInodeData) throws StorageException {
    if(fileInodeData.getDBFileStorageType() != FileInodeData.Type.OnDiskFile){
      throw new IllegalArgumentException("Expecting on disk file object. Got: "+fileInodeData.getDBFileStorageType());
    }
    final HopsSession session = connector.obtainSession();
    session.deletePersistent(MediumOnDiskFileInodeClusterj.FileInodeDataDTO.class, fileInodeData.getInodeId());
  }

  @Override
  public FileInodeData get(long inodeId) throws StorageException {
    final HopsSession session = connector.obtainSession();
    FileInodeDataDTO dataDto = session.find(FileInodeDataDTO.class, inodeId);
    if (dataDto != null) {
      byte[] data = new byte[dataDto.getData().length];
      System.arraycopy(dataDto.getData(),0,data,0,data.length);
      FileInodeData fileData = new FileInodeData(inodeId, data,data.length, FileInodeData.Type.OnDiskFile);
      session.release(dataDto);
      return fileData;
    }

    return null;
  }

  @Override
  public int count() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public int getLength() throws StorageException {
    String query = "SELECT character_maximum_length  FROM information_schema.columns  " +
            "WHERE   table_schema =  Database() AND " +
            "table_name =\""+TABLE_NAME+"\" AND " +
            "column_name = \""+DATA+"\"";

    return MySQLQueryHelper.executeIntAggrQuery(query);
  }
}
