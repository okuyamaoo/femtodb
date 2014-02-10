package femtodb.core.accessor;

import femtodb.core.table.*;


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
    public TableInfo get(String tableName) {
        return this.tableManager.getTableInfo(tableName);
    }

}