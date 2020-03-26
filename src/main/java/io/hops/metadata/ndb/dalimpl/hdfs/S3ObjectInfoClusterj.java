package io.hops.metadata.ndb.dalimpl.hdfs;

import com.google.common.primitives.Longs;
import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.S3ObjectInfoDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectInfo;
import io.hops.metadata.hdfs.entity.S3ObjectLookUp;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.mysqlserver.MySQLQueryHelper;
import io.hops.metadata.ndb.wrapper.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3ObjectInfoClusterj
        implements TablesDef.S3ObjectInfoTableDef, S3ObjectInfoDataAccess<S3ObjectInfo> {

  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface DTO {
    @PrimaryKey
    @Column(name = INODE_ID)
    long getINodeId();

    void setINodeId(long iNodeId);

    @PrimaryKey
    @Column(name = OBJECT_ID)
    long getObjectId();

    void setObjectId(long objectId);

    @Column(name = OBJECT_INDEX)
    int getObjectIndex();

    void setObjectIndex(int objectIndex);

    @Column(name = REGION)
    String getRegion();

    void setRegion(String region);

    @Column(name = BUCKET)
    String getBucket();

    void setBucket(String bucket);

    @Column(name = KEY)
    String getKey();

    void setKey(String key);

    @Column(name = VERSION_ID)
    String getVersionId();

    void setVersionId(String versionId);

    @Column(name = NUM_BYTES)
    long getNumBytes();

    void setNumBytes(long numBytes);

    @Column(name = CHECKSUM)
    long getChecksum();

    void setChecksum(long checksum);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public int countAll() throws StorageException {
    return MySQLQueryHelper.countAll(TABLE_NAME);
  }

  @Override
  public S3ObjectInfo findById(long objectId, long inodeId) throws StorageException {
    Object[] pk = new Object[2];
    pk[0] = inodeId;
    pk[1] = objectId;

    HopsSession session = connector.obtainSession();
    DTO s3ObjTable =
            session.find(DTO.class, pk);
    if (s3ObjTable == null) {
      return null;
    }

    S3ObjectInfo s3Obj = createS3ObjectInfo(s3ObjTable);
    session.release(s3ObjTable);

    return s3Obj;
  }

  @Override
  public List<S3ObjectInfo> findByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DTO> dobj =
            qb.createQueryDefinition(DTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<DTO> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", inodeId);
    List<DTO> dtos = query.getResultList();
    List<S3ObjectInfo> modelList = createS3ObjectInfoList(dtos);
    session.release(dtos);
    return modelList;
  }

  @Override
  public List<S3ObjectInfo> findByInodeIds(long[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DTO> dobj =
            qb.createQueryDefinition(DTO.class);
    HopsPredicate pred1 = dobj.get("iNodeId").equal(dobj.param("iNodeParam"));
    dobj.where(pred1);
    HopsQuery<DTO> query = session.createQuery(dobj);
    query.setParameter("iNodeParam", Longs.asList(inodeIds));

    List<DTO> dtos = query.getResultList();
    List<S3ObjectInfo> modelList = createS3ObjectInfoList(dtos);
    session.release(dtos);
    return modelList;
  }

  @Override
  public List<S3ObjectInfo> findAllS3Objects() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DTO> dobj =
            qb.createQueryDefinition(DTO.class);
    HopsQuery<DTO> query = session.createQuery(dobj);

    List<DTO> dtos = query.getResultList();
    List<S3ObjectInfo> modelList = createS3ObjectInfoList(dtos);
    session.release(dtos);
    return modelList;
  }

  @Override
  public List<S3ObjectInfo> findByIds(long[] objectIds, long[] inodeIds) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<S3ObjectInfo> modelList = readS3ObjectInfoBatch(session, inodeIds, objectIds);
    return modelList;
  }

  @Override
  public void add(S3ObjectInfo s3ObjectInfo) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = session.newInstance(DTO.class);
    createPersistable(s3ObjectInfo, row);
    session.savePersistent(row);
    session.release(row);
  }

  @Override
  public void deleteAll(List<S3ObjectInfo> objects) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> toDelete = new ArrayList<>();
    List<S3ObjectDeletableClusterj.DTO> deletables = new ArrayList<>();
    try {
      for(S3ObjectInfo object : objects) {
        Object[] pk = new Object[2];
        pk[0] = object.getInodeId();
        pk[1] = object.getObjectId();
        DTO row = session.newInstance(DTO.class, pk);
        toDelete.add(row);

        // Create pending deletable records
        S3ObjectDeletableClusterj.DTO delDto = session.newInstance(S3ObjectDeletableClusterj.DTO.class);
        S3ObjectDeletableClusterj.createPersistable(object, delDto);
        session.makePersistent(delDto);
        deletables.add(delDto);
      }
      session.deletePersistentAll(toDelete);
      session.savePersistentAll(deletables);
    } finally {
      session.release(toDelete);
    }

  }

  @Override
  public void prepare(Collection<S3ObjectInfo> removed, Collection<S3ObjectInfo> news,
                      Collection<S3ObjectInfo> modified) throws StorageException {
    List<DTO> objChanges = new ArrayList<>();
    List<DTO> objDeletions = new ArrayList<>();
    List<S3ObjectLookUpClusterj.S3ObjectLookUpDTO> luChanges = new ArrayList<>();
    List<S3ObjectLookUpClusterj.S3ObjectLookUpDTO> luDeletions = new ArrayList<>();
    List<S3ObjectDeletableClusterj.DTO> delChanges = new ArrayList<>();

    HopsSession session = connector.obtainSession();
    try {
      for (S3ObjectInfo s3Object : removed) {
        Object[] pk = new Object[2];
        pk[0] = s3Object.getInodeId();
        pk[1] = s3Object.getObjectId();

        DTO s3ObjTable =
                session.newInstance(DTO.class, pk);
        objDeletions.add(s3ObjTable);

        S3ObjectLookUpClusterj.S3ObjectLookUpDTO luDto =
                session.newInstance(S3ObjectLookUpClusterj.S3ObjectLookUpDTO.class, s3Object.getObjectId());
        luDeletions.add(luDto);

        // Create pending deletable records
        S3ObjectDeletableClusterj.DTO delDto = session.newInstance(S3ObjectDeletableClusterj.DTO.class);
        S3ObjectDeletableClusterj.createPersistable(s3Object, delDto);
        session.makePersistent(delDto);
        delChanges.add(delDto);
      }

      for (S3ObjectInfo s3Object : news) {
        DTO s3ObjTable =
                session.newInstance(DTO.class);
        createPersistable(s3Object, s3ObjTable);
        objChanges.add(s3ObjTable);

        S3ObjectLookUpClusterj.S3ObjectLookUpDTO luDto =
                session.newInstance(S3ObjectLookUpClusterj.S3ObjectLookUpDTO.class);
        S3ObjectLookUpClusterj.createPersistable(
                new S3ObjectLookUp(s3Object.getObjectId(), s3Object.getInodeId()), luDto);
        luChanges.add(luDto);
      }

      for (S3ObjectInfo s3Object : modified) {
        DTO s3ObjTable =
                session.newInstance(DTO.class);
        createPersistable(s3Object, s3ObjTable);
        objChanges.add(s3ObjTable);

        S3ObjectLookUpClusterj.S3ObjectLookUpDTO luDto =
                session.newInstance(S3ObjectLookUpClusterj.S3ObjectLookUpDTO.class);
        S3ObjectLookUpClusterj.createPersistable(
                new S3ObjectLookUp(s3Object.getObjectId(), s3Object.getInodeId()), luDto);
        luChanges.add(luDto);
      }

      session.deletePersistentAll(objDeletions);
      session.deletePersistentAll(luDeletions);
      session.savePersistentAll(objChanges);
      session.savePersistentAll(luChanges);
      session.savePersistentAll(delChanges);
    }finally {
      session.release(objDeletions);
      session.release(luDeletions);
      session.release(objChanges);
      session.release(luChanges);
      session.release(delChanges);
    }
  }

  private List<S3ObjectInfo> readS3ObjectInfoBatch(final HopsSession session,
                                             final long[] inodeIds, final long[] objectIds) throws StorageException {
    final List<DTO> dtos = new ArrayList<>();
    try {
      for (int i = 0; i < objectIds.length; i++) {
        Object[] pk = new Object[]{inodeIds[i], objectIds[i]};
        DTO dto =
                session.newInstance(DTO.class, pk);
        dto.setObjectId(NOT_FOUND_ROW);
        dto = session.load(dto);
        dtos.add(dto);
      }
      session.flush();
      List<S3ObjectInfo> modelList = createS3ObjectInfoList(dtos);
      return modelList;
    }finally{
      session.release(dtos);
    }
  }

  private List<S3ObjectInfo> createS3ObjectInfoList(
          List<DTO> s3ObjDTOList) {
    List<S3ObjectInfo> list = new ArrayList<>();
    if (s3ObjDTOList != null) {
      for (DTO s3ObjDTO : s3ObjDTOList) {
        if (s3ObjDTO.getObjectIndex() != NOT_FOUND_ROW) {
          list.add(createS3ObjectInfo(s3ObjDTO));
        }
      }
    }
    return list;
  }

  private S3ObjectInfo createS3ObjectInfo(DTO s3ObjDTO) {
    S3ObjectInfo s3ObjectInfo =
            new S3ObjectInfo(s3ObjDTO.getObjectId(), s3ObjDTO.getObjectIndex(), s3ObjDTO.getINodeId(),
                    s3ObjDTO.getRegion(), s3ObjDTO.getBucket(), s3ObjDTO.getKey(), s3ObjDTO.getVersionId(),
                    s3ObjDTO.getNumBytes(), s3ObjDTO.getChecksum());
    return s3ObjectInfo;
  }

  private void createPersistable(S3ObjectInfo s3Object, DTO persistable) {
    persistable.setINodeId(s3Object.getInodeId());
    persistable.setObjectId(s3Object.getObjectId());
    persistable.setObjectIndex(s3Object.getObjectIndex());
    persistable.setRegion(s3Object.getRegion());
    persistable.setBucket(s3Object.getBucket());
    persistable.setKey(s3Object.getKey());
    persistable.setVersionId(s3Object.getVersionId());
    persistable.setNumBytes(s3Object.getNumBytes());
    persistable.setChecksum(s3Object.getChecksum());
  }
}
