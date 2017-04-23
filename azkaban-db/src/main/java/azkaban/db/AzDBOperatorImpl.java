package azkaban.db;

import java.sql.Connection;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import com.google.inject.Inject;


/**
 * Implement AZ DB related operations. This class is thread safe.
 */
public class AzDBOperatorImpl implements AzDBOperator {

  private static final Logger logger = Logger.getLogger(AzDBOperatorImpl.class);

  private AzkabanDataSource dataSource;

  @Inject
  public AzDBOperatorImpl(AzkabanDataSource dataSource){
    this.dataSource = dataSource;
  }

  /**
   * The ID that was generated is maintained in Mysql server on a per-connection basis.
   * This means that the value returned by the function to a given client is
   * the first AUTO_INCREMENT value generated for most recent statement
   *
   * This value cannot be affected by other callers, even if they generate
   * AUTO_INCREMENT values of their own.
   * @return last insertion ID
   *
   * TODO: not sure if we need to clase connection in the end.
   */
  @Override
  public Long getLastInsertId() {
    QueryRunner run = new QueryRunner();
    Connection conn = null;
    long num = -1;
    try {
      // A default connection: autocommit = true.
      conn = dataSource.getConnection();
      num = ((Number) run.query(conn,"SELECT LAST_INSERT_ID();", new ScalarHandler<>(1))).longValue();
    } catch (SQLException ex) {
      logger.error("can not get last insertion ID", ex);
    } finally {
      DbUtils.closeQuietly(conn);
    }
    return num;
  }

  @Override
  public <T, V extends Throwable> T query(String basequery,
      ResultSetHandler<T> resultHandler,
      AzDBExceptionWrapper<V> exceptionWrapper,
      Object... params) throws V {

    QueryRunner run = new QueryRunner();
    Connection conn = null;
    try{
      conn = dataSource.getConnection();
      return run.query(conn, basequery, resultHandler, params);
    } catch (SQLException ex){
      logger.error("query failed", ex);
      throw getExceptionInstance(exceptionWrapper);
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  @Override
  public <T, V extends Throwable> T transaction(SQLTransaction<T, V> operations,
      AzDBExceptionWrapper<V> exceptionWrapper) throws V {
    Connection conn = null;
    try{
      conn = dataSource.getConnection();
      conn.setAutoCommit(false);
      T res = operations.execute(conn);
      conn.commit();
      return res;
    } catch (Throwable ex) {
      logger.error("transaction failed", ex);
      throw getExceptionInstance(exceptionWrapper);
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  @Override
  public <V extends Throwable> int update(String updateClause,
      AzDBExceptionWrapper<V> exceptionWrapper,
      Object...params) throws V {
    QueryRunner run = new QueryRunner();
    Connection conn = null;
    try{
      conn = dataSource.getConnection();
      return run.update(conn, updateClause, params);
    } catch (SQLException ex){
      logger.error("update failed", ex);
      throw getExceptionInstance(exceptionWrapper);
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  private <V extends Throwable> V getExceptionInstance(String exceptionMessage,
      Class<V> callerExceptionClass){
    try {
      return callerExceptionClass.getConstructor(String.class).newInstance(exceptionMessage);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | SecurityException | NoSuchMethodException e) {
      e.printStackTrace();
      // rethrow
    }

    return null;
  }

  private <V extends Throwable> V getExceptionInstance(AzDBExceptionWrapper<V> exceptionWrapper){
    return exceptionWrapper.create();
  }
}
