package azkaban.db;

import org.apache.commons.dbutils.QueryRunner;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;


public class AzDBTransOperatorImplTest {

  AzDBTransOperator operator;
  AzkabanDataSource datasource;

  @Before
  public void setUp() throws Exception {
    datasource = new AzDBTestUtility.EmbeddedH2BasicDataSource();
    this.operator = new AzDBTransOperatorImpl(new QueryRunner(), datasource.getConnection());
  }

  @Ignore @Test
  public void query() throws Exception {
  }

  @Ignore @Test
  public void update() throws Exception {
  }
}