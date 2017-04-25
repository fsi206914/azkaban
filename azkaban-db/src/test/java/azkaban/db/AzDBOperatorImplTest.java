package azkaban.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.*;

public class AzDBOperatorImplTest {

  private AzkabanDataSource datasource = new AzDBTestUtility.EmbeddedH2BasicDataSource();

  private AzDBOperator dbOperator;
  private QueryRunner queryRunner;
  private Connection conn;

  ResultSetHandler<Integer> handler = rs -> {
    if (!rs.next()) {
      return 0;
    }

    return rs.getInt(1);
  };

  private ScalarHandler<Object> scalarHandler = new ScalarHandler<>(1);

  private static final List<Integer> list = new ArrayList<>();

  private static int index_1 = 3;
  private static int index_2 = 15;

  @Before
  public void setUp() throws Exception {
    queryRunner = mock(QueryRunner.class);

    list.add(index_1);
    list.add(index_2);
    when(queryRunner.query(any(String.class), any(ScalarHandler.class))).thenReturn(2L);
    when(queryRunner.query("select * from blah where ? = ?", handler, "id", 2)).thenReturn(index_2);

    // If select an non-existing entry, handler returns 0.
    when(queryRunner.query("select * from blah where ? = ?", handler, "id", 3)).thenReturn(0);

    //Typo
    doThrow(SQLException.class).when(queryRunner).query("sele * from blah where ? = ?", handler, "id", 2);

    doAnswer(new Answer<Integer>() {
      public Integer answer(InvocationOnMock invocation) {
        index_1 = 26;
        return 1;
      }
    }).when(queryRunner).update("update blah set ? = ?", "1", 26);

    this.dbOperator = new AzDBOperatorImpl(queryRunner);

    conn = datasource.getConnection();
  }

  @Test
  public void testGetLastInsertId() throws Exception {
    long as = dbOperator.getLastInsertId();
    Assert.assertEquals(2L, as);
  }

  @Test
  public void query() throws Exception {
    int res = dbOperator.query("select * from blah where ? = ?", handler, "id", 2);
    Assert.assertEquals(15, res);
    verify(queryRunner).query("select * from blah where ? = ?", handler, "id", 2);
  }

  @Test
  public void invalidQuery() throws Exception {
    int res = dbOperator.query("select * from blah where ? = ?", handler, "id", 3);
    Assert.assertEquals(0, res);
  }

  @Test(expected = SQLException.class)
  public void testTypoSqlStatement() throws Exception {
    System.out.println("testTypoSqlStatement");
    dbOperator.query("sele * from blah where ? = ?", handler, "id", 2);
  }


  @Test
  public void transaction() throws Exception {

    DataSource mockDataSource = mock(datasource.getClass());

    when(queryRunner.getDataSource()).thenReturn(mockDataSource);
    when(mockDataSource.getConnection()).thenReturn(conn);

    when(queryRunner.update(conn,"update blah set ? = ?", "1", 26)).thenReturn(1);
    when(queryRunner.query(conn,"select * from blah where ? = ?", handler, "id", 1)).thenReturn(26);

    SQLTransaction<Integer> transaction = transOperator -> {
      transOperator.update("update blah set ? = ?", "1", 26);
      return transOperator.query("select * from blah where ? = ?", handler, "id", 1);
    };

    int res = dbOperator.transaction(transaction);
    Assert.assertEquals(26, res);
  }

  @Test
  public void update() throws Exception {
    int res = dbOperator.update("update blah set ? = ?", "1", 26);

    // 1 row is affected
    Assert.assertEquals(1, res);
    Assert.assertEquals(26, index_1);
    verify(queryRunner).update("update blah set ? = ?", "1", 26);
  }

  @Test
  public void inValidupdate() throws Exception {
    int res = dbOperator.update("update blah set ? = ?", "3", 26);

    // 0 row is affected
    Assert.assertEquals(0, res);
  }
}