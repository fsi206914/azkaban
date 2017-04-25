package azkaban.db;

import java.sql.Connection;
import org.apache.commons.dbutils.DbUtils;
import org.apache.log4j.Logger;

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

  private final QueryRunner queryRunner;

  @Inject
  public AzDBOperatorImpl(QueryRunner queryRunner){
    this.queryRunner = queryRunner;

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
   */
  @Override
  public Long getLastInsertId() {
    // A default connection: autocommit = true.
    long num = -1;
    try {
      num = ((Number) queryRunner.query("SELECT LAST_INSERT_ID();", new ScalarHandler<>(1))).longValue();
    } catch (SQLException ex) {
      logger.error("can not get last insertion ID", ex);
    }
    // QeuryRunner closes SQL connection in default.
    return num;
  }

  @Override
  public <T> T query(String baseQuery,
      ResultSetHandler<T> resultHandler,
      Object...params) throws SQLException {

    try{
      return queryRunner.query(baseQuery, resultHandler, params);
    } catch (SQLException ex){
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("query failed", ex);
      throw ex;
    }
  }

  @Override
  public <T> T transaction(SQLTransaction<T> operations) throws SQLException {
    Connection conn = null;
    try{
      conn = queryRunner.getDataSource().getConnection();
      conn.setAutoCommit(false);
      AzDBTransOperator transOperator = new AzDBTransOperatorImpl(queryRunner, conn);
      T res = operations.execute(transOperator);
      conn.commit();
      return res;
    } catch (SQLException ex) {
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("transaction failed", ex);
      throw ex;
    } finally {
      DbUtils.closeQuietly(conn);
    }
  }

  @Override
  public int update(String updateClause,
      Object...params) throws SQLException {
    try{
      return queryRunner.update(updateClause, params);
    } catch (SQLException ex){
      // todo kunkun-tang: Retry logics should be implemented here.
      logger.error("update failed", ex);
      throw ex;
    }
  }
}
