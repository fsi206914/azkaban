package azkaban.db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;


/**
 * This class should be only used inside {@link AzDBTransOperatorImpl}, then we remove public.
 */
class AzDBTransOperatorImpl implements AzDBTransOperator {

  private Connection conn;

  public AzDBTransOperatorImpl(Connection conn) {
    this.conn = conn;
  }

  @Override
  public <T> T query(String querySql, ResultSetHandler<T> resultHandler, Object... params) throws SQLException {
    QueryRunner run = new QueryRunner();
    try{
      return run.query(conn, querySql, resultHandler, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  @Override
  public int update(String updateClause, Object... params) throws SQLException {
    QueryRunner run = new QueryRunner();
    try{
      return run.update(conn, updateClause, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }
}
