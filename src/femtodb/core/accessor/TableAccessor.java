package femtodb.core.accessor;

import java.util.*;

import femtodb.core.table.*;


/** 
 * TableAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TableAccessor {

    private TableManager tableManager = null;

    public static int TABLE_CREATE_EXIST_ERROR = 2;


    public TableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public TableInfo getTableInfo(String tableName) {
        return this.tableManager.getTableInfo(tableName);
    }
    public int create(TableInfo info) {

        synchronized(this.tableManager.tableCreateObj) {
            if (!this.tableManager.existTable(info.tableName)) {

                int ret = this.tableManager.create(info);
                return ret;
            } else {

                // 既にテーブルが存在する
                return TableAccessor.TABLE_CREATE_EXIST_ERROR;
            }
        }
    }

    public boolean rebuildIndex(String tableName) {
        ITable table = this.tableManager.getTableData(tableName);
        if (table == null) return false;
        table.rebuildIndex();
        return true;
    }

    public boolean cleanDeletedData(String tableName) {
        ITable table = this.tableManager.getTableData(tableName);
        if (table == null) return false;
        table.cleanDeletedData();
        return true;
    }

    public List<TableInfo> getTableList() {
        return this.tableManager.getTableInfoList();
    }

    public TableInfo get(String tableName) {
        return this.tableManager.getTableInfo(tableName);
    }

    public TableInfo remove(String tableName) {
        TableInfo tableInfo = this.tableManager.removeTableInfo(tableName);
        if (tableInfo == null) return null;
        
        ITable table = this.tableManager.removeTableData(tableName);
        return tableInfo;
    }

}