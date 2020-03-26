package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Longs;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.S3ObjectLookUpDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectLookUp;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.HopsSession;

import java.util.*;

public class S3ObjectLookUpClusterj
  implements TablesDef.S3ObjectLookUpTableDef, S3ObjectLookUpDataAccess<S3ObjectLookUp> {

  @PersistenceCapable(table = TABLE_NAME)
  public interface S3ObjectLookUpDTO {
    @PrimaryKey
    @Column(name = OBJECT_ID)
    long getObjectId();

    void setObjectId(long objectId);

    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long inodeId);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static long NOT_FOUND_ROW = -1000L;

  @Override
  public S3ObjectLookUp findByObjectId(long objectId) throws StorageException {
    HopsSession session = connector.obtainSession();
    S3ObjectLookUpDTO dto = session.find(S3ObjectLookUpDTO.class, objectId);
    if(dto == null) {
      return null;
    }
    S3ObjectLookUp model = createModel(dto);
    session.release(dto);
    return model;
  }

  @Override
  public long[] findINodeIdsByObjectIds(long[] objectIds) throws StorageException {
    final HopsSession session = connector.obtainSession();
    return readINodeIdsByObjectIds(session, objectIds);
  }

  protected static long[] readINodeIdsByObjectIds(final HopsSession session, final long[] objectIds)
    throws StorageException {
    final List<S3ObjectLookUpDTO> dtos = new ArrayList<>();
    final List<Long> inodeIds = new ArrayList<>();
    try {
      for(long objectId : objectIds) {
        S3ObjectLookUpDTO dto = session.newInstance(S3ObjectLookUpDTO.class, objectId);
        dto.setINodeId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();

      for(S3ObjectLookUpDTO dto : dtos) {
        if(dto.getINodeId() != NOT_FOUND_ROW) {
          inodeIds.add(dto.getINodeId());
        } else {
          S3ObjectLookUpDTO dton = session.find(S3ObjectLookUpDTO.class, dto.getObjectId());
          if(dton != null) {
            inodeIds.add(dton.getINodeId());
            session.release(dton);
          } else {
            inodeIds.add(NOT_FOUND_ROW);
          }
        }
      }

      return Longs.toArray(inodeIds);
    } finally {
      session.release(dtos);
    }
  }

  @Override
  public void prepare(Collection<S3ObjectLookUp> modified, Collection<S3ObjectLookUp> removed)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    List<S3ObjectLookUpDTO> changes = new ArrayList<>();
    List<S3ObjectLookUpDTO> deletions = new ArrayList<>();

    try {
      for(S3ObjectLookUp model : removed) {
        S3ObjectLookUpDTO dto = session.newInstance(S3ObjectLookUpDTO.class, model.getObjectId());
        deletions.add(dto);
      }

      for(S3ObjectLookUp model : modified) {
        S3ObjectLookUpDTO dto = session.newInstance(S3ObjectLookUpDTO.class);
        createPersistable(model, dto);
        changes.add(dto);
      }

      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  @Override
  public Map<Long, List<Long>> getINodeIdsForObjectIds(long[] objectIds) throws StorageException {
    final HopsSession session = connector.obtainSession();
    final List<S3ObjectLookUpDTO> dtos = new ArrayList<>();
    final Map<Long, List<Long>> inodeToObjectIdsMap = new HashMap<>(objectIds.length);

    try{
      for(long objectId : objectIds) {
        S3ObjectLookUpDTO dto = session.newInstance(S3ObjectLookUpDTO.class, objectId);
        dto.setINodeId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();

      for(S3ObjectLookUpDTO dto : dtos) {
        if(dto.getINodeId() != NOT_FOUND_ROW) {
          addObjectId(inodeToObjectIdsMap, dto);
        } else {
          S3ObjectLookUpDTO dton = session.find(S3ObjectLookUpDTO.class, dto.getObjectId());
          if(dton != null) {
            addObjectId(inodeToObjectIdsMap, dton);
            session.release(dton);
          }
        }
      }

      return inodeToObjectIdsMap;
    } finally {
      session.release(dtos);
    }
  }

  private void addObjectId(Map<Long, List<Long>> map, S3ObjectLookUpDTO dto) {
    List<Long> objectIds = map.get(dto.getINodeId());
    if(objectIds == null) {
      objectIds = new ArrayList<>();
      map.put(dto.getINodeId(), objectIds);
    }
    objectIds.add(dto.getObjectId());
  }

  private S3ObjectLookUp createModel(S3ObjectLookUpDTO dto) {
    return new S3ObjectLookUp(dto.getObjectId(), dto.getINodeId());
  }

  protected static void createPersistable(S3ObjectLookUp model, S3ObjectLookUpDTO dto) {
    dto.setObjectId(model.getObjectId());
    dto.setINodeId(dto.getINodeId());
  }
}
