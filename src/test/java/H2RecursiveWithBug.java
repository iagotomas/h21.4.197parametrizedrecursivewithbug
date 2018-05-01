import static org.hamcrest.CoreMatchers.is;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


/*
If tests are run in the same database depending on which order are run will fail both, to show the unexpected behaviour of parametrized inner queries
inside a with statement comparing the same query for both methods separate databases are employed
 - If preparedStatement is called before createStatement with troubling query neither work
 - If createStatement is run before preparedStatement with troubling query only simple statement works
 - If ran on different connection, sessions, transactions preparedStatement always fails
 */
public class H2RecursiveWithBug
{
    // JDBC driver name and database URL
    static final String JDBC_DRIVER = "org.h2.Driver";
    static final String DB_URL = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false;MODE=Oracle;MVCC=false";
    static final String DB2_URL = "jdbc:h2:mem:testdb2;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=false;MODE=Oracle;MVCC=false";

    static final String createQuery = "CREATE TABLE HIERARCHY(id int primary key, parentid int)";
    static final String createQuery2 = "CREATE TABLE SIMPLETABLE(id int primary key, value CHAR(25))";
    static final String insertQuery = "INSERT INTO HIERARCHY(id, parentid) values (?,?)";
    static final String insertQuery2 = "INSERT INTO SIMPLETABLE(id, value) values (?,?)";
    //  Database credentials
    static final String USER = "sa";
    static final String PASS = "";

    static final String sql =
        "WITH dummy(id) AS ("
            + "SELECT id FROM HIERARCHY WHERE id=? " // Issue with parametrized inner query
            + "UNION ALL "
            + "SELECT h.id FROM dummy d INNER JOIN HIERARCHY h on d.id=h.parentid"
            + "), "
            + "dummy2(cid) AS ("
            + "SELECT h.id from dummy d INNER JOIN SIMPLETABLE h on d.id=h.id"
            + ") "
            + "SELECT "
            + "s.id "
            + "FROM dummy s ";

    static final String sqlStatic = sql.replaceFirst("\\?", "1");
    private static final String FIELD_FORMAT = "%32s|";
    private static final int RESULTS_COUNT = 5;

    private Connection conn = null;


    @Before
    public void setUp() throws ClassNotFoundException, SQLException
    {
        Class.forName(JDBC_DRIVER);
        System.out.println("Connecting to database...");

    }

    @Test
    public void testPrepareStatement() throws SQLException, ClassNotFoundException
    {
        try(Connection conn = DriverManager.getConnection(DB_URL, USER, PASS))
        {
            createTables(conn);
            System.out.println("Using prepared statement");
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql))
            {
                preparedStatement.setInt(1, 1);
                try (ResultSet rs = preparedStatement.executeQuery())
                {
                    System.out.println("SQL prepared statement query executed...");

                    Assert.assertThat(readResult(rs), is(RESULTS_COUNT));
                }
            }
        }
    }

    @Test
    public void testCreateStatement() throws SQLException
    {
        try(Connection conn = DriverManager.getConnection(DB2_URL, USER, PASS))
        {
            createTables(conn);
            try (Statement stmt = conn.createStatement())
            {

                try (ResultSet rs = stmt.executeQuery(sqlStatic))
                {
                    System.out.println("SQL statement query executed... ");
                    Assert.assertThat(readResult(rs), is(RESULTS_COUNT));
                }
            }
        }
    }

    /**
     * Create some random data
     * @throws SQLException
     */
    private void createTables(Connection conn) throws SQLException
    {
        try (Statement stmt = conn.createStatement())
        {
            stmt.execute(createQuery);
            stmt.execute(createQuery2);
            stmt.close();
        }

        try (PreparedStatement preparedStatement = conn.prepareStatement(insertQuery))
        {
            preparedStatement.setInt(1, 1);
            preparedStatement.setInt(2, 0);
            preparedStatement.execute();
            preparedStatement.setInt(1, 2);
            preparedStatement.setInt(2, 1);
            preparedStatement.execute();
            preparedStatement.setInt(1, 3);
            preparedStatement.setInt(2, 1);
            preparedStatement.execute();
            preparedStatement.setInt(1, 4);
            preparedStatement.setInt(2, 2);
            preparedStatement.execute();
            preparedStatement.setInt(1, 5);
            preparedStatement.setInt(2, 4);
            preparedStatement.execute();
            preparedStatement.close();
        }
        try (PreparedStatement preparedStatement = conn.prepareStatement(insertQuery2))
        {
            preparedStatement.setInt(1, 2);
            preparedStatement.setString(2, "somevalue");
            preparedStatement.execute();
            preparedStatement.setInt(1, 3);
            preparedStatement.setString(2, "othervalue");
            preparedStatement.execute();
            preparedStatement.setInt(1, 4);
            preparedStatement.setString(2, "somemore");
            preparedStatement.execute();
            preparedStatement.setInt(1, 5);
            preparedStatement.setString(2, "...");
            preparedStatement.execute();
            preparedStatement.close();
        }


    }

    /**
     * Retrieve a column name preventing checked exceptions
     * @param index the index of the column to retrieve
     * @param resultSetMetaData the result set metadata to retrieve it from
     * @return a column name
     */
    private static String columnName(final int index, final ResultSetMetaData resultSetMetaData)
    {
        try
        {
            return resultSetMetaData.getColumnName(index);
        }
        catch (final SQLException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Read the result set printing it to stdout and returning the result count
     * @param rs the result set to read
     * @return the result count
     * @throws SQLException
     */
    private static int readResult(final ResultSet rs) throws SQLException
    {
        int count = 0;
        final ResultSetMetaData rsmd = rs.getMetaData();
        final int columns = rsmd.getColumnCount();

        System.out.print("   | ");
        IntStream.range(1, columns + 1)
            .mapToObj(index -> String.format(FIELD_FORMAT, columnName(index, rsmd)))
            .forEach(System.out::print);
        System.out.println();
        System.out.println(new String(new char[(columns * 32) + 7]).replace("\0", "-"));
        while (rs.next())
        {
            // Display values
            System.out.print(String.format(" %d | ", count));
            IntStream.range(1, columns + 1)
                .mapToObj(i -> String.format(FIELD_FORMAT, getField(rs, i)))
                .forEach(System.out::print);
            System.out.println();

            count++;
        }
        System.out.println("Results size: " + count);
        rs.close();
        return count;
    }

    /**
     * Get a field value avoiding checked exceptions
     * @param rs the result set to retrieve the calue from
     * @param i the column index
     * @return the value as a string
     */
    private static String getField(final ResultSet rs, final int i)
    {
        try
        {
            return rs.getString(i);
        }
        catch (final SQLException e)
        {
            throw new RuntimeException(e);
        }
    }
}
