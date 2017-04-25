package azkaban.db;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;


public class AzDBTestUtility {

  public static class EmbeddedH2BasicDataSource extends AzkabanDataSource {

    public EmbeddedH2BasicDataSource() {
      super();
      String url = "jdbc:h2:mem:test";
      setDriverClassName("org.h2.Driver");
      setUrl(url);
    }

    @Override
    public String getDBType() {
      return "h2-in-memory";
    }
  }

  public static class MockQueryRunner extends QueryRunner {

    private final Object mockObj = new Object();

    public Object query(String sql, Object... params) throws SQLException {
      return mockObj;
    }

    public int update(String sql, Object... params) throws SQLException {
      return 0;
    }
  }
}
