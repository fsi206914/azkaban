package com.linkedin.azkaban.db;

import java.sql.SQLException;

@FunctionalInterface
public interface SQLSupplier<T> {
  public T execute() throws SQLException;
}
