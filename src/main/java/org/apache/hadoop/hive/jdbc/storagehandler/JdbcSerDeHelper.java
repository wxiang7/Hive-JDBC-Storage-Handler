/*
 * Copyright 2013-2015 Qubole
 * Copyright 2013-2015 Makoto YUI
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.jdbc.storagehandler;

import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.SerDeException;
import java.sql.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

public class JdbcSerDeHelper {

    private static final Log LOG = LogFactory.getLog(JdbcSerDe.class);
    public static String columnNames;
    public static String columnTypeNames;
    private Connection dbConnection;
    private String tableName;

    public void initialize(Properties tblProps, Configuration sysConf) {
        setProperties(tblProps, sysConf);
        StringBuilder colNames = new StringBuilder();
        StringBuilder colTypeNames = new StringBuilder();
        DBConfiguration dbConf = new DBConfiguration(sysConf);
        try {

            dbConnection = dbConf.getConnection();
            String query = getSelectQuery(tblProps);
            Statement st = dbConnection.createStatement();
            ResultSet rs = st.executeQuery(query);
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();

            int i = 0;
            for (i = 1; i < columnsNumber; i++) {
                String colName = rsmd.getColumnName(i);
                int colType = rsmd.getColumnType(i);
                colNames.append(colName + ",");
                colTypeNames.append(sqlTypeToHiveColumnTypeNames(colType) + ":");
            }
            colNames.append(rsmd.getColumnName(i));
            colTypeNames.append(rsmd.getColumnTypeName(i));

            columnNames = colNames.toString();
            columnTypeNames = colTypeNames.toString();

            rs.close();
            st.close();
            dbConnection.close();

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void setProperties(Properties tblProps, Configuration sysConf) {

        for (String key : tblProps.stringPropertyNames()) {
            // LOG.info(">> " + key + ">> " + tblProps.getProperty(key));
            if (key.contains("jdbc.input.table.name")) {
                String value = tblProps.getProperty(key);
                tableName = value;
            }
            if (key.startsWith("mapred.jdbc.")) {
                String value = tblProps.getProperty(key);
                sysConf.set(key, value);
                key = key.replaceAll("mapred", "mapreduce");
                sysConf.set(key, value);
            }

        }
        for (String key : tblProps.stringPropertyNames()) {
            if (key.startsWith("mapreduce.jdbc.")) {
                String value = tblProps.getProperty(key);
                sysConf.set(key, value);
                key = key.replaceAll("mapreduce", "mapred");
                sysConf.set(key, value);
            }
        }
    }

    public String getSelectQuery(Properties tblProps) {
        StringBuilder query = new StringBuilder();
        query.append("Select * from ");
        query.append(tableName);
        query.append(" LIMIT 1");
        LOG.info(">> " + query.toString());
        return query.toString();
    }

    public String sqlTypeToHiveColumnTypeNames(int sqlType) throws SerDeException {
        switch (sqlType) {
            case Types.VARCHAR:
                return "STRING";
            case Types.LONGVARCHAR:
                return "STRING";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.TINYINT:
                return "TINYINT";
            case Types.BIT:
                return "BOOLEAN";
            case Types.SMALLINT:
                return "SMALLINT";
            case Types.INTEGER:
                return "INT";
            case Types.BIGINT:
                return "BIGINT";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.DATE:
                return "DATE";
            case Types.BINARY:
                return "BINARY";
            case Types.ARRAY:
                return "ARRAY<";
            default:
                throw new SerDeException("Unrecognized column type: " + sqlType);
        }
    }

    public String getColumnNames() {
        return columnNames;
    }

    public String getColumnTypeNames() {
        return columnTypeNames;
    }

    public static void setFilters(JobConf jobConf) {
        String filterConditions = jobConf
                .get(TableScanDesc.FILTER_TEXT_CONF_STR);
        String condition = jobConf
                .get(DBConfiguration.INPUT_CONDITIONS_PROPERTY);
        if (filterConditions != null && condition != null) {
            condition = condition.concat(" AND ");
            condition = condition.concat(filterConditions);
        } else if (filterConditions != null) {
            condition = filterConditions;
        }
        LOG.info("FilterPushDown Conditions: " + condition);
        if (condition != null) {
            jobConf.set(DBConfiguration.INPUT_CONDITIONS_PROPERTY, condition);
        }
    }
}
