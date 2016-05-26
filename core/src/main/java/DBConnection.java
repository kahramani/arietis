import oracle.sql.TIMESTAMP;
import oracle.sql.TIMESTAMPLTZ;
import oracle.sql.TIMESTAMPTZ;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;

/**
 * Created by kahramani on 5/24/2016.
 */
public class DBConnection {
    private Connection connection;
    private Properties connectionProps;
    private String url;
    private String query;
    private PreparedStatement preparedStatement;

    public DBConnection(String url, String query, String userName, String password, String dbName) {
        this.connectionProps = new Properties();
        this.url = url;
        this.query = query;
        this.connectionProps.put("user", userName);
        this.connectionProps.put("password", password);
        if(dbName != null && !dbName.trim().equals(""))
            this.connectionProps.put("databaseName", dbName);
    }

    public void setPreparedStatement() {
        /**
         * to set prepared statement for connection
         */
        try {
            this.preparedStatement = this.connection.prepareStatement(this.query);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public Connection open(String jdbcDriverClass) {
        /**
         * to open connection to db
         */
        try {
            Class.forName(jdbcDriverClass);
            System.out.println("Trying to connect to " + this.url);
            this.connection = DriverManager.getConnection(this.url, this.connectionProps);
            setPreparedStatement();
            System.out.println("Connection established to " + this.url);
        } catch (Exception e) {
            System.out.println("Failed to open connection to " + this.url);
            e.printStackTrace();
        }
        return this.connection;
    }

    public void close() {
        /**
         * to close connection with db
         */
        try {
            this.connection.close();
            System.out.println("Connection closed with " + this.url);
        } catch (Exception e) {
            System.out.println("Failed to close connection with " + this.url);
            e.printStackTrace();
        }
    }

    public ResultSet executeQuery(String query, int fetchSize) {
        /**
         * to execute a query with given fetch size
         */
        ResultSet rs = null;
        try {
            System.out.println("Query is about to execute. <" + query + ">");
            if(fetchSize != -1)
                this.preparedStatement.setFetchSize(fetchSize);
            rs = this.preparedStatement.executeQuery();
            System.out.println("Query executed...");
        } catch (Exception e) {
            System.out.println("Failed to execute query.");
            e.printStackTrace();
        }
        return rs;
    }

    public int[] operateWithBatch(ArrayList<LinkedHashMap> bulkList, LinkedHashMap<String, String> columnMap) throws SQLException {
        /**
         * to insert or update values from bulkList to columns which are kept in columnMap with their data types.
         *
         */
        int[] count = null;
        try {
            this.connection.setAutoCommit(false);
            int k = 0;
            for (; k < bulkList.size(); k++) {
                LinkedHashMap map = bulkList.get(k);
                Iterator it = map.entrySet().iterator();
                int counter = 0;
                while(it.hasNext()) {
                    counter++;
                    Map.Entry pair = (Map.Entry) it.next();
                    String key = (String) pair.getKey();
                    String dataType = columnMap.get(key);
                    setPSValue(counter, dataType, pair.getValue());
                }
                this.preparedStatement.addBatch();
            }
            count = this.preparedStatement.executeBatch();
            this.connection.commit();
        } catch (Exception e) {
            System.out.println("Failed to execute batch.");
            e.printStackTrace();
        } finally {
            this.preparedStatement.clearBatch();
        }
        return count;
    }

    private void setPSValue(int counter, String dataType, Object value) {
        /**
         * to cast objects to column data types and set prepared statement values for batching
         */
        try {
            if (dataType.equalsIgnoreCase("oracle.sql.TIMESTAMP")) {
                if (value == null)
                    this.preparedStatement.setNull(counter, Types.TIMESTAMP);
                else
                    this.preparedStatement.setTimestamp(counter, ((TIMESTAMP) value).timestampValue());
            } else if (dataType.equalsIgnoreCase("oracle.sql.TIMESTAMPLTZ")) {
                if (value == null)
                    this.preparedStatement.setNull(counter, Types.TIMESTAMP);
                else
                    this.preparedStatement.setTimestamp(counter, TIMESTAMPLTZ.toTimestamp(this.connection, ((TIMESTAMPLTZ) value).toBytes()));
            } else if (dataType.equalsIgnoreCase("oracle.sql.TIMESTAMPTZ")) {
                if (value == null)
                    this.preparedStatement.setNull(counter, Types.TIMESTAMP);
                else
                    this.preparedStatement.setTimestamp(counter, TIMESTAMPTZ.toTimestamp(this.connection, ((TIMESTAMPTZ) value).toBytes()));
            } else {
                if (dataType.toUpperCase().contains("INTEGER")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.INTEGER);
                    else
                        this.preparedStatement.setInt(counter, ((Integer) value).intValue());
                } else if (dataType.toUpperCase().contains("BIGDECIMAL")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.DECIMAL);
                    else
                        this.preparedStatement.setBigDecimal(counter, (BigDecimal) value);
                } else if (dataType.toUpperCase().contains("FLOAT")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.FLOAT);
                    else
                        this.preparedStatement.setFloat(counter, (Float) value);
                } else if (dataType.toUpperCase().contains("DOUBLE")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.DOUBLE);
                    else
                        this.preparedStatement.setDouble(counter, (Double) value);
                } else if (dataType.toUpperCase().contains("BOOLEAN")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.BOOLEAN);
                    else
                        this.preparedStatement.setBoolean(counter, (Boolean) value);
                } else if (dataType.toUpperCase().contains("DATE")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.DATE);
                    else
                        this.preparedStatement.setDate(counter, (Date) value);
                } else if (dataType.toUpperCase().contains("TIMESTAMP")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.TIMESTAMP);
                    else
                        this.preparedStatement.setTimestamp(counter, (Timestamp) value);
                } else if (dataType.toUpperCase().contains("TIME")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.TIME);
                    else
                        this.preparedStatement.setTime(counter, (Time) value);
                } else if (dataType.toUpperCase().contains("ARRAY")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.ARRAY);
                    else
                        this.preparedStatement.setArray(counter, (Array) value);
                } else if (dataType.toUpperCase().contains("CLOB")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.CLOB);
                    else
                        this.preparedStatement.setClob(counter, (Clob) value);
                } else if (dataType.toUpperCase().contains("BLOB")) {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.BLOB);
                    else
                        this.preparedStatement.setBlob(counter, (Blob) value);
                } else {
                    if (value == null)
                        this.preparedStatement.setNull(counter, Types.VARCHAR);
                    else
                        this.preparedStatement.setString(counter, (String) value);
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to set preparedStatement value.");
            e.printStackTrace();
        }
    }
}
