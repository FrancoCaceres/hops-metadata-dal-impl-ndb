package io.hops.metadata.ndb.dalimpl.hdfs;

import com.mysql.clusterj.annotation.Column;
import com.mysql.clusterj.annotation.PartitionKey;
import com.mysql.clusterj.annotation.PersistenceCapable;
import com.mysql.clusterj.annotation.PrimaryKey;
import io.hops.exception.StorageException;
import io.hops.metadata.hdfs.TablesDef;
import io.hops.metadata.hdfs.dal.S3ProcessableDataAccess;
import io.hops.metadata.hdfs.entity.S3Processable;
import io.hops.metadata.ndb.ClusterjConnector;
import io.hops.metadata.ndb.wrapper.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class S3ProcessableClusterj
        implements TablesDef.S3ProcessableTableDef, S3ProcessableDataAccess<S3Processable> {
  @PersistenceCapable(table = TABLE_NAME)
  @PartitionKey(column = INODE_ID)
  public interface DTO {
    @PrimaryKey
    @Column(name = INODE_ID)
    long getInodeId();
    void setInodeId(long inodeId);

    @Column(name = RESCHEDULE_AT)
    Long getRescheduleAt();
    void setRescheduleAt(Long rescheduleAt);

    @Column(name = SCHEDULED_FOR)
    String getScheduledFor();
    void setScheduledFor(String scheduledFor);
  }

  private ClusterjConnector connector = ClusterjConnector.getInstance();

  @Override
  public S3Processable findByInodeId(long inodeId) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = session.find(DTO.class, inodeId);
    if(row == null) {
      return null;
    }

    S3Processable processable = createObj(row);
    session.release(row);
    return processable;
  }

  @Override
  public List<S3Processable> getNFirstAvailableForScheduling(int n, long currentTimestamp) throws StorageException {
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
    List<S3Processable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    return objs;
  }

  @Override
  public List<S3Processable> findAllS3Processables() throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();
    HopsQueryDomainType<DTO> qdef = qb.createQueryDefinition(DTO.class);
    HopsQuery<DTO> query = session.createQuery(qdef);
    List<DTO> rows = query.getResultList();
    List<S3Processable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    session.release(rows);
    return objs;
  }

  @Override
  public List<S3Processable> getScheduledFor(String address) throws StorageException {
    HopsSession session = connector.obtainSession();
    HopsQueryBuilder qb = session.getQueryBuilder();

    final String scheduledForColParam = "scheduledFor";
    HopsQueryDomainType<DTO> qdef = qb.createQueryDefinition(DTO.class);
    HopsPredicate pred = qdef.get(scheduledForColParam).equal(qdef.param(scheduledForColParam));
    qdef.where(pred);

    HopsQuery<DTO> query = session.createQuery(qdef);
    query.setParameter(scheduledForColParam, address);
    List<DTO> rows = query.getResultList();
    List<S3Processable> objs = new ArrayList<>();
    for(DTO row : rows) {
      objs.add(createObj(row));
    }
    session.release(rows);
    return objs;
  }

  @Override
  public void updateAll(List<S3Processable> processables) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> toPersist = new ArrayList<>();

    for(S3Processable processable : processables) {
      DTO row = session.newInstance(DTO.class);
      createPersistable(row, processable);
      toPersist.add(row);
    }

    session.savePersistentAll(toPersist);
    session.release(toPersist);
  }

  @Override
  public void add(S3Processable processable) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = session.newInstance(DTO.class);
    createPersistable(row, processable);
    session.savePersistent(row);
    session.release(row);
  }

  @Override
  public void delete(S3Processable processable) throws StorageException {
    HopsSession session = connector.obtainSession();
    DTO row = null;
    try {
      row = session.newInstance(DTO.class);
      createPersistable(row, processable);
      session.deletePersistent(row);
    } finally {
      session.release(row);
    }
  }

  @Override
  public void prepare(Collection<S3Processable> removed, Collection<S3Processable> news,
                      Collection<S3Processable> modified) throws StorageException {
    HopsSession session = connector.obtainSession();
    List<DTO> changes = new ArrayList<>();
    List<DTO> deletions = new ArrayList<>();

    try {
      for(S3Processable obj : removed) {
        DTO row = session.newInstance(DTO.class);
        createPersistable(row, obj);
        changes.add(row);
      }

      for(S3Processable obj : removed) {
        DTO row = session.newInstance(DTO.class, obj.getInodeId());
        deletions.add(row);
      }

      for(S3Processable obj : modified) {
        DTO row = session.newInstance(DTO.class);
        createPersistable(row, obj);
        changes.add(row);
      }

      session.deletePersistentAll(deletions);
      session.savePersistentAll(changes);
    } finally {
      session.release(deletions);
      session.release(changes);
    }
  }

  private void createPersistable(DTO row, S3Processable obj) {
    row.setInodeId(obj.getInodeId());
    row.setRescheduleAt(obj.getRescheduleAt());
    row.setScheduledFor(obj.getScheduledFor());
  }

  private S3Processable createObj(DTO row) {
    return new S3Processable(row.getInodeId(), row.getRescheduleAt(), row.getScheduledFor());
  }
}
