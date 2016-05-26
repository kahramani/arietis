import java.util.Properties;

/**
 * Created by kahramani on 5/24/2016.
 */
public class TargetDB {
    private String url;
    private String username;
    private String password;
    private String jdbcDriver;
    private String dbName;
    private int bulkOperationCount;
    private StringBuilder query;

    public TargetDB() {
    }

    public TargetDB(Properties prop) {
        if(prop.getProperty("TARGET_DB_URL") != null)
            this.url = prop.getProperty("TARGET_DB_URL");
        if(prop.getProperty("TARGET_DB_USERNAME") != null)
            this.username = prop.getProperty("TARGET_DB_USERNAME");
        if(prop.getProperty("TARGET_DB_PASSWORD") != null)
            this.password = prop.getProperty("TARGET_DB_PASSWORD");
        if(prop.getProperty("TARGET_DB_JDBC_DRIVER") != null)
            this.jdbcDriver = prop.getProperty("TARGET_DB_JDBC_DRIVER");
        if(prop.getProperty("TARGET_DB_NAME") != null)
            this.dbName = prop.getProperty("TARGET_DB_NAME");
        if(prop.getProperty("BULK_OPERATION_COUNT") != null)
            this.bulkOperationCount = Integer.parseInt(prop.getProperty("BULK_OPERATION_COUNT"));
        if(prop.getProperty("TARGET_DB_TEMPLATE_QUERY_FILE") != null)
            this.query = Utility.getSQLQueryFromFile(prop.getProperty("TARGET_DB_TEMPLATE_QUERY_FILE"));
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

    public int getBulkOperationCount() {
        return bulkOperationCount;
    }

    public void setBulkOperationCount(int bulkOperationCount) {
        this.bulkOperationCount = bulkOperationCount;
    }

    public StringBuilder getQuery() {
        return query;
    }

    public void setQuery(StringBuilder query) {
        this.query = query;
    }

}
