/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.jdbc.storagehandler;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.db.*;
import org.apache.hadoop.conf.Configuration;


public class JdbcDBInputFormat<T extends DBWritable> extends DBInputFormat {

    private static final Log LOG = LogFactory.getLog(JdbcDBInputFormat.class);

    private String localConditions;

    private String localTableName;

    private String[] localFieldNames;

    private DBConfiguration localDbConf;

    public JdbcDBInputFormat(){
    }

    public void setConfLocal(){
        localDbConf = super.getDBConf();
        localTableName = localDbConf.getInputTableName();
        localFieldNames = localDbConf.getInputFieldNames();
        localConditions = localDbConf.getInputConditions();
    }

    protected void setParentConnection(Connection connection) throws NoSuchFieldException, IllegalAccessException {
        Field field = getClass().getSuperclass().getDeclaredField("connection");
        field.setAccessible(true);
        field.set(this, connection);
        field.setAccessible(false);
    }

    protected Connection getParentConnection() throws NoSuchFieldException, IllegalAccessException {
        Field field = getClass().getSuperclass().getDeclaredField("connection");
        field.setAccessible(true);
        return (Connection) (field.get(this));
    }

    @Override
    public Connection getConnection() {
        try {
            Connection conn = getParentConnection();
            if (conn == null || conn.isClosed()) {
                LOG.warn("Connection is closed or null, creating new connection");
                setParentConnection(null);
                conn = super.getConnection();
            }
            return conn;
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RecordReader<LongWritable, T> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {  
        setConfLocal();
        Class<T> inputClass = (Class<T>) (localDbConf.getInputClass());
        try{
            LOG.info("DB Type >> " + super.getDBProductName());
            String dbProductName = super.getDBProductName().toUpperCase();
            Configuration conf = localDbConf.getConf();
            if (dbProductName.startsWith("MICROSOFT SQL SERVER")) {
                return new MicrosoftDBRecordReader<T>((DBInputFormat.DBInputSplit)split, inputClass,
                localDbConf.getConf(), getConnection(), super.getDBConf(), localConditions, localFieldNames,
                        localTableName);
            } else if (dbProductName.startsWith("ORACLE")) {
                // use Oracle-specific db reader.
                return new OracleDBRecordReader<T>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), localConditions, localFieldNames,
                        localTableName);
            } else if (dbProductName.startsWith("MYSQL")) {
                // use MySQL-specific db reader.
                return new MySQLDBRecordReaderEnhanced<T>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), localConditions, localFieldNames,
                        localTableName);
            } else {
                // Generic reader.
                return new GenericDBRecordReader<>((DBInputFormat.DBInputSplit) split, inputClass,
                        conf, getConnection(), getDBConf(), localConditions, localFieldNames,
                        localTableName);
            }
        }catch (SQLException ex) {
           throw new IOException(ex.getMessage());
        }
    }

    /** {@inheritDoc} */
    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {

        ResultSet results = null;
        Statement statement = null;
        Connection connection = getConnection();
        try {
            statement = connection.createStatement();

            results = statement.executeQuery(getCountQuery());
            results.next();

            long count = results.getLong(1);
            int chunks = job.getConfiguration().getInt("mapreduce.job.maps", 1);
            long chunkSize = (count / chunks);

            results.close();
            statement.close();

            List<InputSplit> splits = new ArrayList<InputSplit>();

            // Split the rows into n-number of chunks and adjust the last chunk
            // accordingly
            for (int i = 0; i < chunks; i++) {
                DBInputSplit split;

                if ((i + 1) == chunks)
                    split = new DBInputSplit(i * chunkSize, count);
                else
                    split = new DBInputSplit(i * chunkSize, (i * chunkSize)
                            + chunkSize);

                splits.add(split);
            }

            connection.commit();
            return splits;
        } catch (Exception e) {
            LOG.error("Got exception when creating splits for table");
            throw new IOException("Got Exception", e);
        } finally {
            try {
                if (results != null) { results.close(); }
            } catch (SQLException e1) {}
            try {
                if (statement != null) { statement.close(); }
            } catch (SQLException e1) {}

            closeConnection();
        }
    }

    @Override
    protected void closeConnection() {
        try {
            LOG.info("Closing JDBC connection");
            Connection conn = getParentConnection();
            if (null != conn) {
                conn.close();
                setParentConnection(null);
            }
        } catch (Exception e) {
            LOG.warn("Exception on close", e);
        }
    }

}