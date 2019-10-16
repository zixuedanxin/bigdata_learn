package org.apache.spark.sql.mlsql.sources.mysql.binlog;

import java.io.Serializable;

/**
 * 2019-06-14 WilliamZhu(allwefantasy@gmail.com)
 */
public class TableInfo implements Serializable {

    private String databaseName;
    private String tableName;
    private Long tableId;
    private String schema;

    public TableInfo(String databaseName, String tableName, Long tableId, String schema) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tableId = tableId;
        this.schema = schema;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Long getTableId() {
        return tableId;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
