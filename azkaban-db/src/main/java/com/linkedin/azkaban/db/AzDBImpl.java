package com.linkedin.azkaban.db;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import com.google.inject.Inject;

public class AzDBImpl implements AzBaseDAO{

  private static final Logger logger = Logger.getLogger(AzDBImpl.class);

  private BasicDataSource dataSource;

  @Inject
  public AzDBImpl(BasicDataSource basicDataSource){
    dataSource = basicDataSource;
  }

  @Override
  public Long getLastInsertId() throws SQLException {
    QueryRunner run = new QueryRunner(dataSource);
    return ((Number)run.query("SELECT LAST_INSERT_ID();",
        new ScalarHandler<>(1))).longValue();
  }

  @Override
  public <T, V extends Throwable> T query(String basequery, ResultSetHandler<T> resultHandler, String exceptionMessage,
      Class<V> callerExceptionClass, Object... params) throws V {

    QueryRunner run = new QueryRunner(dataSource);
    try{
      T res= run.query(basequery, resultHandler, params);
      return res;
    } catch (SQLException ex){
      throw getExceptionInstance(exceptionMessage, callerExceptionClass);
    }
  }

  @Override
  public <T, V extends Throwable> T transaction(SQLSupplier<T, V> operations, String exceptionMessage,
      Class<V> callerExceptionClass) throws V {
    try{
      return operations.execute();
    } catch (Throwable ex) {
      logger.error(exceptionMessage, ex);
      if ( callerExceptionClass.isInstance(ex) ) {
        throw ex;
      } else {
        throw getExceptionInstance(exceptionMessage, callerExceptionClass);
      }
    }
  }

  @Override
  public <V extends Throwable> void update(String updateClause, String exceptionMessage, Class<V> callerExceptionClass,
      Object...params) throws V {
    QueryRunner run = new QueryRunner(dataSource);
    try{
      run.update(updateClause, params);
    } catch (SQLException ex){
      throw getExceptionInstance(exceptionMessage, callerExceptionClass);
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
}
