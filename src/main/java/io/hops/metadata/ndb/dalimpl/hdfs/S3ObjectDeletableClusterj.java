package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.S3ObjectDeletableDataAccess;
import io.hops.metadata.hdfs.entity.S3ObjectDeletable;
import io.hops.metadata.hdfs.entity.S3ObjectInfo;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3ObjectDeletableClusterj
        implements TablesDef.S3ObjectDeletableTableDef, S3ObjectDeletableDataAccess<S3ObjectDeletable> {
  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = ID)
  public interface DTO {
    @PrimaryKey
    @Column(name = ID)
    long getId();
    void setId(long id);

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

    @Column(name = RESCHEDULE_AT)
    Long getRescheduleAt();

    void setRescheduleAt(Long rescheduleAt);

    @Column(name = SCHEDULED_FOR)
    String getScheduledFor();

    void setScheduledFor(String scheduledFor);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();
  private final static int NOT_FOUND_ROW = -1000;

  @Override
  public S3ObjectDeletable findById(long id) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = session.find(DTO.class, id);
    if(row == null) {
      return null;
    }
    S3ObjectDeletable obj = createObj(row);
    session.release(row);
    return obj;
  }

  // TODO FCG: Use sql timestamp or application-provided?
  @Override
  public List<S3ObjectDeletable> getNFirstAvailableForScheduling(int n, long currentTimestamp)
          throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    final String rescheduleAtColParam = "rescheduleAt";
    final String currentTimestampParam = "ts";
    HopsQueryDomainType<DTO> qdef = qb.createQueryDefinition(DTO.class);
    HopsPredicate pred = qdef.get(rescheduleAtColParam).equal(qdef.param(rescheduleAtColParam));
    pred = pred.or(qdef.get(rescheduleAtColParam).greaterEqual(qdef.param(currentTimestampParam)));
    qdef.where(pred);

    HopsQuery<DTO> query = session.createQuery(qdef);
    query.setParameter(rescheduleAtColParam, null);
    query.setParameter(currentTimestampParam, currentTimestamp);
    query.setLimits(0, n);

    List<DTO> rows = query.getResultList();
    List<S3ObjectDeletable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    return objs;
  }

  @Override
  public List<S3ObjectDeletable> getAll() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DTO> qdef = qb.createQueryDefinition(DTO.class);
    HopsQuery<DTO> query = session.createQuery(qdef);
    List<DTO> rows = query.getResultList();
    List<S3ObjectDeletable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    session.release(rows);
    return objs;
  }

  @Override
  public List<S3ObjectDeletable> getScheduledFor(String address) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    final String scheduledForColParam = "scheduledFor";
    HopsQueryDomainType<DTO> qdef = qb.createQueryDefinition(DTO.class);
    HopsPredicate pred = qdef.get(scheduledForColParam).equal(qdef.param(scheduledForColParam));
    qdef.where(pred);

    HopsQuery<DTO> query = session.createQuery(qdef);
    query.setParameter(scheduledForColParam, address);
    List<DTO> rows = query.getResultList();
    List<S3ObjectDeletable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    session.release(rows);
    return objs;
  }

  @Override
  public void updateAll(List<S3ObjectDeletable> deletables) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> toPersist = new ArrayList<>();

    for(S3ObjectDeletable deletable : deletables) {
      DTO row = session.newInstance(DTO.class);
      createPersistable(deletable, row);
      toPersist.add(row);
    }

    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void delete(S3ObjectDeletable deletable) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = null;
    try {
      row = session.newInstance(DTO.class);
      createPersistable(deletable, row);
      session.deletePersistent(row);
    } finally {
      session.release(row);
    }
  }

  @Override
  public void deleteAll(List<S3ObjectDeletable> deletables) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> toDelete = new ArrayList<>();

    for(S3ObjectDeletable deletable : deletables) {
      DTO row = session.newInstance(DTO.class);
      createPersistable(deletable, row);
      toDelete.add(row);
    }

    session.deletePersistentAll(toDelete);
    session.release(toDelete);
  }

  @Override
  public void prepare(Collection<S3ObjectDeletable> removed, Collection<S3ObjectDeletable> news,
                      Collection<S3ObjectDeletable> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> changes = new ArrayList<>();
    List<DTO> deletions = new ArrayList<>();

    try {
      for(S3ObjectDeletable obj : news) {
        DTO row = session.newInstance(DTO.class);
        createPersistable(obj, row);
        changes.add(row);
      }

      for(S3ObjectDeletable obj : removed) {
        DTO row = session.newInstance(DTO.class, obj.getId());
        deletions.add(row);
      }

      for(S3ObjectDeletable obj : modified) {
        DTO row = session.newInstance(DTO.class);
        createPersistable(obj, row);
        changes.add(row);
      }

      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  static void createPersistable(S3ObjectDeletable obj, DTO row) {
    row.setId(obj.getId());
    row.setRegion(obj.getRegion());
    row.setBucket(obj.getBucket());
    row.setKey(obj.getKey());
    row.setVersionId(obj.getVersionId());
    row.setNumBytes(obj.getNumBytes());
    row.setRescheduleAt(obj.getRescheduleAt());
    row.setScheduledFor(obj.getScheduledFor());
  }

  static void createPersistable(S3ObjectInfo obj, DTO row) {
    row.setRegion(obj.getRegion());
    row.setBucket(obj.getBucket());
    row.setKey(obj.getKey());
    row.setVersionId(obj.getVersionId());
    row.setNumBytes(obj.getNumBytes());
  }

  private S3ObjectDeletable createObj(DTO row) {
    return new S3ObjectDeletable(row.getId(), row.getRegion(), row.getBucket(), row.getKey(), row.getVersionId(),
            row.getNumBytes(), row.getRescheduleAt(), row.getScheduledFor());
  }
}
