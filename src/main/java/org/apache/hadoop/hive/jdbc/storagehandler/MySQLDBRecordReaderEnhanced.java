package org.apache.hadoop.hive.jdbc.storagehandler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.db.MySQLDBRecordReader;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLDBRecordReaderEnhanced<T extends DBWritable> extends MySQLDBRecordReader<T> {

    private static final Log LOG = LogFactory.getLog(MySQLDBRecordReaderEnhanced.class);

    public MySQLDBRecordReaderEnhanced(DBInputFormat.DBInputSplit split,
                               Class<T> inputClass, Configuration conf, Connection conn, DBConfiguration dbConfig,
                               String cond, String [] fields, String table) throws SQLException {
        super(split, inputClass, conf, conn, dbConfig, cond, fields, table);
    }

    @Override
    protected ResultSet executeQuery(String query) throws SQLException {
        LOG.info("JDBC Query to MYSQL: " + query);
        PreparedStatement statement = getConnection().prepareStatement(query,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(Integer.MIN_VALUE); // MySQL: read row-at-a-time.
        return statement.executeQuery();
    }


    /**
     * Wrap column name with backtick
     */
    @Override
    protected String getSelectQuery() {
        StringBuilder query = new StringBuilder();
        DBConfiguration dbConf = getDBConf();
        String[] fieldNames = getFieldNames();
        String tableName = getTableName();
        String conditions = getConditions();
        DBInputFormat.DBInputSplit split = getSplit();

        // Default codepath for MySQL, HSQLDB, etc. Relies on LIMIT/OFFSET for splits.
        if(dbConf.getInputQuery() == null) {
            query.append("SELECT ");

            for (int i = 0; i < fieldNames.length; i++) {
                query.append("`");
                query.append(fieldNames[i]);
                query.append("`");
                if (i != fieldNames.length -1) {
                    query.append(", ");
                }
            }

            query.append(" FROM ").append(tableName);
            query.append(" AS ").append(tableName); //in hsqldb this is necessary
            if (conditions != null && conditions.length() > 0) {
                query.append(" WHERE (").append(conditions).append(")");
            }

            String orderBy = dbConf.getInputOrderBy();
            if (orderBy != null && orderBy.length() > 0) {
                query.append(" ORDER BY ").append(orderBy);
            }
        } else {
            //PREBUILT QUERY
            query.append(dbConf.getInputQuery());
        }

        try {
            query.append(" LIMIT ").append(split.getLength());
            query.append(" OFFSET ").append(split.getStart());
        } catch (IOException ex) {
            // Ignore, will not throw.
        }

        return query.toString();
    }
}
