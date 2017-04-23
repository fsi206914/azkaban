/*
 * Copyright 2017 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package azkaban.db;

import org.apache.commons.dbutils.ResultSetHandler;

/**
 * This interface is to define Base Data Access Object contract for Azkaban. All azkaban
 * DB related operations must be performed upon this interface. AZ DB operators all leverages
 * QueryRunner interface.
 *
 * @see org.apache.commons.dbutils.QueryRunner
 */
public interface AzDBOperator {

  /**
   * returns the last id from a previous insert statement.
   * Note: The implementation must catch SQL Exception itself.
   *
   * @return the last inserted id in mysql per connection.
   */
  Long getLastInsertId();

  /**
   * Executes the given AZ related SELECT SQL statements.
   *
   * @param baseQuery The SQL query statement to execute.
   * @param resultHandler The handler used to create the result object
   * @param exceptionWrapper the exception message and type to be thrown if needed
   * @param params Initialize the PreparedStatement's IN parameters
   * @param <T> The type of object that the qeury handler returns
   * @param <V> The Exception class type
   * @return The object returned by the handler.
   * @throws V Expected Exception type specified by the caller
   */
  <T, V extends Throwable> T query(String baseQuery,
      ResultSetHandler<T> resultHandler,
      AzDBExceptionWrapper<V> exceptionWrapper,
      Object...params) throws V;

  /**
   *
   * @param operations A collection of DB operations
   * @param exceptionWrapper the exception message and type to be thrown if needed
   * @param <T> The type of object that the operations returns. Note that T could be null
   * @param <V> The exception type expected by the user
   * @return T The object returned by the SQL statement, expected by the caller
   * @throws V Expected Exception type specified by the caller
   */
  <T, V extends Throwable> T transaction(SQLTransaction<T, V> operations, AzDBExceptionWrapper<V> exceptionWrapper) throws V;

  /**
   * Executes the given AZ related INSERT, UPDATE, or DELETE SQL statement.
   *
   * @param updateClause sql statements to execute
   * @param exceptionWrapper the exception message and type to be thrown if needed
   * @param params Initialize the PreparedStatement's IN parameters
   * @param <V> The exception type expected by the user
   * @return The number of rows updated.
   * @throws V Expected Exception type specified by the caller
   */
  <V extends Throwable> int update(String updateClause,
      AzDBExceptionWrapper<V> exceptionWrapper,
      Object...params) throws V;
}
