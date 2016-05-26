import java.util.Properties;

/**
 * Created by kahramani on 5/24/2016.
 */
public class SourceDB {
    private String url;
    private String username;
    private String password;
    private String jdbcDriver;
    private String dbName;
    private int fetchSize;
    private StringBuilder query;

    public SourceDB() {
    }

    public SourceDB(Properties prop) {
        if(prop.getProperty("SOURCE_DB_URL") != null)
            this.url = prop.getProperty("SOURCE_DB_URL");
        if(prop.getProperty("SOURCE_DB_USERNAME") != null)
            this.username = prop.getProperty("SOURCE_DB_USERNAME");
        if(prop.getProperty("SOURCE_DB_PASSWORD") != null)
            this.password = prop.getProperty("SOURCE_DB_PASSWORD");
        if(prop.getProperty("SOURCE_DB_JDBC_DRIVER") != null)
            this.jdbcDriver = prop.getProperty("SOURCE_DB_JDBC_DRIVER");
        if(prop.getProperty("SOURCE_DB_NAME") != null)
            this.dbName = prop.getProperty("SOURCE_DB_NAME");
        if(prop.getProperty("FETCH_SIZE") != null)
            this.fetchSize = Integer.parseInt(prop.getProperty("FETCH_SIZE"));
        if(prop.getProperty("SOURCE_DB_SELECT_QUERY_FILE") != null)
            this.query = Utility.getSQLQueryFromFile(prop.getProperty("SOURCE_DB_SELECT_QUERY_FILE"));
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    public StringBuilder getQuery() {
        return query;
    }

    public void setQuery(StringBuilder query) {
        this.query = query;
    }
}
