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

  private final Connection conn;
  private final QueryRunner queryRunner;

  public AzDBTransOperatorImpl(QueryRunner queryRunner, Connection conn) {
    this.conn = conn;
    this.queryRunner= queryRunner;
  }

  @Override
  public <T> T query(String querySql, ResultSetHandler<T> resultHandler, Object... params) throws SQLException {
    try{
      return queryRunner.query(conn, querySql, resultHandler, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }

  @Override
  public int update(String updateClause, Object... params) throws SQLException {
    try{
      return queryRunner.update(conn, updateClause, params);
    } catch (SQLException ex){
      //RETRY Logic should be implemented here if needed.
      throw ex;
    } finally {
      // Note: CAN NOT CLOSE CONNECTION HERE.
    }
  }
}
