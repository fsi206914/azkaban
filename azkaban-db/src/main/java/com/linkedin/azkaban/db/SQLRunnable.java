package com.linkedin.azkaban.db;

import java.sql.SQLException;

@FunctionalInterface
public interface SQLRunnable<T> {
  public void execute() throws SQLException;

}
