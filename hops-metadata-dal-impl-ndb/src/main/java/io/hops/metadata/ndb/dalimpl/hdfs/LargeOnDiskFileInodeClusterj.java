package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.LargeOnDiskInodeDataAccess;
import io.hops.metadata.hdfs.entity.FileInodeData;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.HopsSession;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by salman on 3/10/16.
 */
public class LargeOnDiskFileInodeClusterj
        implements TablesDef.FileInodeLargeDiskData, LargeOnDiskInodeDataAccess<FileInodeData> {
  static final Logger LOG = Logger.getLogger(LargeOnDiskFileInodeClusterj.class);
  private ClusterjConnector connector = ClusterjConnector.getInstance();


  final int CHUNK_SIZE=8000;

  @PersistenceCapable(table = TABLE_NAME)
  public interface FileInodeDataDTO {

    @PrimaryKey
    @Column(name = ID)
    long getInodeId();
    void setInodeId(long inodeId);


    @Column(name = INDEX)
    int getIndex();
    void setIndex(int index);

    @Column(name = DATA)
    byte[] getData();
    void setData(byte[] data);
  }

  @Override
  public void add(FileInodeData fileInodeData) throws StorageException {
    if(fileInodeData.getDBFileStorageType() != FileInodeData.Type.OnDiskFile){
      throw new IllegalArgumentException("Expecting on disk file object. Got: "+fileInodeData.getDBFileStorageType());
    }

    //while adding a file split the data and store in 8000 byte chunks

    final HopsSession session = connector.obtainSession();

    int rows = (int)Math.ceil(fileInodeData.getSize()/((double)CHUNK_SIZE));
    List<FileInodeDataDTO> dtos = new ArrayList<FileInodeDataDTO>();
    for(int index = 0; index < rows; index++){
      byte[] buffer = new byte[CHUNK_SIZE];
      int byteWritten = index*CHUNK_SIZE;
      int toWrite = fileInodeData.getSize() - byteWritten;
      if(toWrite > CHUNK_SIZE){
        toWrite = CHUNK_SIZE;
      }
      System.arraycopy(fileInodeData.getInodeData(), index*CHUNK_SIZE, buffer, 0, toWrite);
      FileInodeDataDTO dto = session.newInstance(LargeOnDiskFileInodeClusterj.FileInodeDataDTO.class);
      dto.setInodeId(fileInodeData.getInodeId());
      dto.setIndex(index);
      dto.setData(buffer);
      dtos.add(dto);
    }

    session.savePersistentAll(dtos);
    session.release(dtos);
  }

  @Override
  public void delete(FileInodeData fileInodeData) throws StorageException {
    if(fileInodeData.getDBFileStorageType() != FileInodeData.Type.OnDiskFile){
      throw new IllegalArgumentException("Expecting on disk file object. Got: "+fileInodeData.getDBFileStorageType());
    }
    final HopsSession session = connector.obtainSession();

    for(int index = 0; index < Math.ceil(fileInodeData.getSize()/((double)CHUNK_SIZE)); index++){
      FileInodeDataDTO dto = session.newInstance(LargeOnDiskFileInodeClusterj.FileInodeDataDTO.class);
      dto.setInodeId(fileInodeData.getInodeId());
      dto.setIndex(index);
      session.deletePersistent(dto);
      session.release(dto);
    }
  }

  @Override
  public FileInodeData get(long inodeId) throws StorageException {
    throw new UnsupportedOperationException("The operation is not yet implemented");
  }

  @Override
  public FileInodeData get(long inodeId, int size) throws StorageException {
    final HopsSession session = connector.obtainSession();
    int rows = (int)Math.ceil(size/((double)CHUNK_SIZE));
    FileInodeDataDTO[] dtos = new FileInodeDataDTO[rows];
    for(int index = 0; index < rows ; index++) {
      FileInodeDataDTO dto = session.newInstance(LargeOnDiskFileInodeClusterj.FileInodeDataDTO.class);
      dtos[index] = dto;
      dto.setInodeId(inodeId);
      dto.setIndex(index);
      session.load(dto);
    }

    session.flush();

    byte[] buffer = new byte[size];
    int remaining = size;
    for(int index = 0; index < rows ; index++) {
      int toRead = -1;
      if(remaining >= CHUNK_SIZE){
        toRead  = CHUNK_SIZE;
        remaining -= CHUNK_SIZE;
      } else {
        toRead = remaining;
      }


      // Typical ClusterJ bug :(. Some times the batch op fails to read the
      // data. Retry reading the rows that are supposed to be in the database
      byte data[] = dtos[index].getData();
      if( data.length ==0 ){
        Object pk[] = new Object[2];
        pk[0] = dtos[index].getInodeId();
        pk[1] = dtos[index].getIndex();
        FileInodeDataDTO dto = session.find(FileInodeDataDTO.class, pk);
        if(dto.getData().length == 0){
          throw new IllegalStateException("Failed to read the small files " +
                  "data from database");
        }
        data = dto.getData();
      }
      System.arraycopy(data,0,buffer,index*CHUNK_SIZE, toRead);
    }

    FileInodeData fileData = new FileInodeData(inodeId, buffer, size, FileInodeData.Type.OnDiskFile);
    session.release(dtos);
    return fileData;
  }

  @Override
  public int countUniqueFiles() throws  StorageException{
    return MySQLQueryHelper.countAllUnique(TABLE_NAME, ID);
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

