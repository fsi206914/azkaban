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

import java.sql.SQLException;
import org.apache.commons.dbutils.ResultSetHandler;


public interface AzBaseDAO {

  /**
   * returns the last id from a previous insert statement.
   *
   * @return the last inserted id in mysql per connection.
   * @throws SQLException if persistence errors occur while executing the operation
   */
  public Long getLastInsertId() throws SQLException;

  /**
   *
   * @param baseQuery The SQL statement to execute.
   * @param resultHandler The handler used to create the result object
   * @param exceptionMessage the exception message to be delivered
   * @param callerExceptionClass the exception class type to be thrown
   * @param params
   * @param <T> The type of object that the qeury handler returns
   * @param <V>
   * @return The object returned by the handler.
   * @throws V
   */
  public <T, V extends Throwable> T query(String baseQuery, ResultSetHandler<T> resultHandler,
      String exceptionMessage, Class<V> callerExceptionClass, Object...params) throws V;

  /**
   *
   * @param operations
   * @param exceptionMessage
   * @param callerExceptionClass
   * @param <T>
   * @param <V>
   * @return
   * @throws V
   */
  public <T, V extends Throwable> T transaction(SQLSupplier<T, V> operations, String exceptionMessage,
      Class<V> callerExceptionClass) throws V;

  /**
   *
   * @param updateClause
   * @param exceptionMessage
   * @param callerExceptionClass
   * @param param
   * @param <V>
   * @throws V
   */
  public <V extends Throwable> void update(String updateClause, String exceptionMessage,
      Class<V> callerExceptionClass, Object...param) throws V;
}
