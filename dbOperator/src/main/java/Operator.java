import java.sql.ResultSet;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by kahramani on 5/24/2016.
 */
public class Operator {
    static Logger logger = Utility.getLoggerInstance("Driver.java");
    static Properties prop = Utility.loadProperties("dbOperator\\src\\main\\resources\\connectionParams.properties");

    public static void main(String[] args) {
        /**
         * main method
         */
        operateInsertion();
    }

    private static void operateInsertion() {
        /**
         * to retrieve values from a source db and insert them to a target db
         */
        long tStart = System.currentTimeMillis();
        ResultSet sdResult = null;

        SourceDB sourceDB = new SourceDB(prop);
        TargetDB targetDB = new TargetDB(prop);

        DBConnection sourceDC = new DBConnection(sourceDB.getUrl(), sourceDB.getQuery().toString(),
                sourceDB.getUsername(), sourceDB.getPassword(), sourceDB.getDbName());

        DBConnection targetDC = new DBConnection(targetDB.getUrl(), targetDB.getQuery().toString(),
                targetDB.getUsername(), targetDB.getPassword(), targetDB.getDbName());

        try {
            targetDC.open(targetDB.getJdbcDriver());
            sourceDC.open(sourceDB.getJdbcDriver());

            sdResult = sourceDC.executeQuery(sourceDB.getQuery().toString(), sourceDB.getFetchSize());

            Utility.processDBOperations(sdResult, logger, targetDC, targetDB);

            logger.info("Operation is ended.");
        } catch (Exception e) {
            logger.info("Failed to achieve DB operation.");
            e.printStackTrace();
        } finally {
            long timeElapse = (System.currentTimeMillis() - tStart);
            logger.info("Operation tooks " + Utility.convertTimeInMsToHour(timeElapse));
            sourceDC.close();
            targetDC.close();
        }
    }
}
