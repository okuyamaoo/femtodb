package femtodb.core.accessor;

import java.util.*;

import femtodb.core.accessor.*;
import femtodb.core.table.*;
import femtodb.core.table.type.*;
import femtodb.core.table.transaction.*;


/** 
 * TableAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TableAccessor {

    private TableManager tableManager = null;
    private QueryOptimizer queryOptimizer = null;
    private TransactionNoManager transactionNoManager = null;

    public static int TABLE_CREATE_EXIST_ERROR = 2;


    public TableAccessor(TableManager tableManager, QueryOptimizer queryOptimizer, TransactionNoManager transactionNoManager) {
        this.tableManager = tableManager;
        this.queryOptimizer = queryOptimizer;
        this.transactionNoManager = transactionNoManager;
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

    public boolean addIndexColumn(TableInfo info) {
        synchronized(this.tableManager.tableCreateObj) {
            try {
                if (!this.tableManager.existTable(info.tableName)) {
                    // テーブルが存在しない
                    return false;
                } else {
                    List<Object[]> addIndexColumnList = new ArrayList();
    
                    ITable table = this.tableManager.getTableData(info.tableName);
                    String[] columnNames = info.getIndexColumnNameList();
                    for (String columnName : columnNames) {
    
                        boolean existColumn =false;
                        String[] nowIndexColumns = table.getIndexColumnNames();
                        for (int idx = 0; idx < nowIndexColumns.length; idx++) {
    
                            if (nowIndexColumns[idx].equals(columnName)) {
                                existColumn = true;
                            }
                        }
                        if (existColumn == false) {
    
                            Object[] addInfo = new Object[2];
                            addInfo[0] = columnName;
                            addInfo[1] = info.getColumnType(columnName);
                            table.addIndexColumn(columnName, info.getColumnType(columnName));
                            addIndexColumnList.add(addInfo);
                        }
                    }
                    this.createAllDataIndex(info.tableName);
                    for (Object[] addInfo : addIndexColumnList) {
                        table.addIndexColumnInfo((String)addInfo[0], (IColumnType)addInfo[1]);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    public boolean rebuildIndex(String tableName) {
        synchronized(this.tableManager.tableCreateObj) {
 
            ITable table = this.tableManager.getTableData(tableName);
            if (table == null) return false;
            table.rebuildIndex(queryOptimizer);
        }
        return true;
    }

    public boolean createAllDataIndex(String tableName) {
        TransactionNo tn = transactionNoManager.createAnonymousTransactionNo();

        ITable table = this.tableManager.getTableData(tableName);
        if (table == null) return false;
        table.createAllDataIndex(tn);
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