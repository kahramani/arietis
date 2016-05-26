import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Created by kahramani on 5/24/2016.
 */
public class Utility {

    public static Logger getLoggerInstance(String loggerClass) {
        /**
         * to create instance of logger for a given class
         */
        Logger logger = Logger.getLogger(loggerClass);
        FileHandler fh;
        try {
            fh = new FileHandler("MyLogFile.log");
            logger.addHandler(fh);
            SimpleFormatter formatter = new SimpleFormatter();
            fh.setFormatter(formatter);
        } catch (Exception e) {
            System.out.println("Failed to create logger instance.");
            e.printStackTrace();
        }
        return logger;
    }

    public static Properties loadProperties(String filePath) {
        /**
         * to load properties from a property file to property object
         */
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream(filePath));
        } catch (Exception e) {
            System.out.println("Failed to load properties.");
            e.printStackTrace();
        }
        return prop;
    }

    public static String convertTimeInMsToHour(long timeElapse) {
        /**
         * to convert time (in ms) to a string which is hour, min, sec based.
         */
        String retVal = "0 sec";
        try {
            timeElapse = timeElapse / 1000;
            int hours = (int) timeElapse / 3600;
            int remainder = (int) timeElapse - hours * 3600;
            int mins = remainder / 60;
            remainder = remainder - mins * 60;
            int secs = remainder;
            retVal = hours + " hours " + mins + " minutes " + secs + " seconds";
        } catch(Exception e) {
            System.out.println("Failed to convert time.");
            e.printStackTrace();
        }
        return retVal;
    }

    public static StringBuilder getSQLQueryFromFile(String filePath) {
        /**
         * to get query from an .sql file
         */
        StringBuilder retVal = new StringBuilder("");
        try {
            FileReader fr = new FileReader(new File(filePath));
            BufferedReader in = new BufferedReader(fr);
            String str;
            while ((str = in.readLine()) != null) {
                retVal.append(str);
            }
            in.close();
        } catch (Exception e) {
            System.out.println("Failed to get query from file.");
            e.printStackTrace();
        }
        return retVal;
    }

    public static boolean processDBOperations(ResultSet sdResult, Logger logger, DBConnection targetDC, TargetDB targetDB) {
        /**
         * to map and to process columns, data types and their values
         */
        Calendar cal = Calendar.getInstance();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms");
        logger.info("Operation is started at: " + dateFormat.format(cal.getTime()) + " for template of <" + targetDB.getQuery().toString() + ">");
        boolean operationCleaned = true;
        try {
            ResultSetMetaData rsmd = sdResult.getMetaData();
            LinkedHashMap<String, String> columnMap = new LinkedHashMap<String, String>();
            for(int i = 1; i <= rsmd.getColumnCount(); i++) {
                String columnLabel = rsmd.getColumnLabel(i);
                String columnClassName = rsmd.getColumnClassName(i);
                columnMap.put(columnLabel, columnClassName);
            }
            LinkedHashMap<String, Object> columnValueMap = new LinkedHashMap<String, Object>();
            ArrayList<LinkedHashMap> bulkList = new ArrayList<LinkedHashMap>();
            int countToBulkOperation = 0;
            int counter = 0;
            long lStartTime = System.currentTimeMillis();
            long lEndTime;
            while(sdResult.next()) {
                operationCleaned = false;
                countToBulkOperation++;
                counter++;
                getValueFromResultSet(sdResult, columnMap, columnValueMap);
                bulkList.add(columnValueMap);
                columnValueMap = new LinkedHashMap<String, Object>();
                if (countToBulkOperation == targetDB.getBulkOperationCount()) {
                    targetDC.operateWithBatch(bulkList, columnMap);
                    lEndTime = System.currentTimeMillis();
                    logger.info(countToBulkOperation + " rows are processed in " + (lEndTime - lStartTime) + " ms. Total count becomes " + counter + ". New bulk query is building...");
                    lStartTime = System.currentTimeMillis();
                    countToBulkOperation = 0;
                    bulkList = new ArrayList<LinkedHashMap>();
                    operationCleaned = true;
                }
            }
            logger.info("operationCleaned=" + operationCleaned);
            if(!operationCleaned) {
                targetDC.operateWithBatch(bulkList, columnMap);
                lEndTime = System.currentTimeMillis();
                logger.info(countToBulkOperation + " rows are processed in " + (lEndTime - lStartTime) + " ms. Total count becomes " + counter + ". New bulk query is building...");
                operationCleaned = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.info("Failed to insert bulk queries");
        }
        cal = Calendar.getInstance();
        logger.info("Operation is ended at: " + dateFormat.format(cal.getTime()) + " and operationCleaned=" + operationCleaned + " for template of <" + targetDB.getQuery().toString() + ">");
        return operationCleaned;
    }

    public static void getValueFromResultSet(ResultSet sdResult, LinkedHashMap<String, String> columnMap,
                                             LinkedHashMap<String, Object> columnValueMap) {
        /**
         * to get resultSet values and to put them in a map
         */
        if(columnMap != null) {
            for (String key : columnMap.keySet()) {
                try {
                    columnValueMap.put(key, sdResult.getObject(key));
                } catch (Exception e) {
                    columnValueMap.put(key, null);
                }
            }
        }
    }
}
