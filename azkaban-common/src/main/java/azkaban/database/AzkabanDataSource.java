/*
 * Copyright 2012 LinkedIn Corp.
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
 */

package azkaban.database;

import azkaban.utils.Props;
import java.sql.SQLException;
import java.sql.Connection;
import javax.sql.DataSource;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.SQLNestedException;


public abstract class AzkabanDataSource extends BasicDataSource {

  private AzkabanDataSource dataSource;
  private Props props;

  public abstract boolean allowsOnDuplicateKey();

  public abstract String getDBType();

  public AzkabanDataSource(Props props) {
    this.dataSource = DataSourceUtils.getDataSource(props);
  }

  @Override
  public Connection getConnection() throws SQLException, SQLNestedException {

    if (getDataSource() == null) {
      return createDataSource().getConnection();
    }

    Connection connection = null;
    try {

    } catch (Exception ex) {
      DataSourceUtils.getDataSource(props);
    }

    return connection;
  }


  public DataSource getDataSource() {
    return dataSource;
  }
}
