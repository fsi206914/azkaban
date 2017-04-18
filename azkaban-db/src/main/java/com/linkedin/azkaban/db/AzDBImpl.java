package com.linkedin.azkaban.db;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;


public class AzDBImpl implements AzBaseDAO{

  @Override
  public Long getLastInsertId(Connection conn) throws SQLException {
    QueryRunner run = new QueryRunner();
    return ((Number)run.query(conn, "SELECT LAST_INSERT_ID();",
        new ScalarHandler<>(1))).longValue();
  }

  @Override
  public <T, V extends Throwable> T query(String basequery, ResultSetHandler<T> resultHandler, String exceptionMessage,
      Class<V> callerExceptionClass) throws V {
    throw getExceptionInstance(exceptionMessage, callerExceptionClass);
  }

  @Override
  public <T, V extends Throwable> T transaction(SQLSupplier<T> operations, String exceptionMessage,
      Class<V> callerExceptionClass) throws V {
    try{
      return operations.execute();
    } catch (SQLException exception){
      throw getExceptionInstance(exceptionMessage, callerExceptionClass);
    }
  }

  @Override
  public <V extends Throwable> void update(String updateClause, String exceptionMessage, Class<V> callerExceptionClass)
      throws V {
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
}
