package femtodb.core.table.transaction;

import java.util.*;
import femtodb.core.table.index.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;


public class TransactionNo {

    private boolean commitedFlg = false;
    private boolean rollbackFlg = false;

    private long transactionNo = -1L;

    public Map<String, Map<Long, TableData>> modTableFolder = null;

    public TransactionNo(long transactionNo) {
        this.transactionNo = transactionNo;
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();

    }

    public long TransactionNo() {
        return this.transactionNo;
    }

    public boolean isCommited() {
        return this.commitedFlg;
    }

    public boolean isRollback() {
        return this.rollbackFlg;
    }



    public void rollback() {
        // ここでmodTableFolderとmodDataMapと自身のtransactionNoを使ってTableDataの履歴データの掃除と
        // Indexの更新を行う。
        // Indexデータも作成
        // 変更したテーブル一覧を取得
        
        for (String tableName : modTableFolder.keySet()) {
            Map<Long, TableData> modTableDataMap = modTableFolder.get(tableName);

            for (Iterator it = modTableDataMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry)it.next();

                Long oid = (Long)entry.getKey();
                TableData tableData = (TableData)entry.getValue();
                if (tableData.oldData == null && tableData.newData != null && tableData.newData.getTransactionNo().getTransactionNo() == transactionNo) {
                    // 自身で作成したデー
                    // rollbackで削除する
                    ITable defaultTable = tableData.getParentTable();
                    defaultTable.removeTmpData(oid);
                }
            }
        }

        this.rollbackFlg = true;
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();
    }

    public void commit() {
        // ここでmodTableFolderとmodDataMapと自身のtransactionNoを使ってTableDataの履歴データの掃除と
        // Indexの更新を行う。
        // Indexデータも作成
        // 変更したテーブル一覧を取得
        
        for (String tableName : modTableFolder.keySet()) {
            Map<Long, TableData> modTableDataMap = modTableFolder.get(tableName);

            for (Iterator it = modTableDataMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry)it.next();

                Long oid = (Long)entry.getKey();
                TableData tableData = (TableData)entry.getValue();
                ITable defaultTable = tableData.getParentTable();
                for (int idx = 0; idx < defaultTable.getIndexColumnNames().length; idx++){

                    IndexMap index = defaultTable.getIndexsMap().get(defaultTable.getIndexColumnNames()[idx]);
                    index.putData(this, tableData.oid, tableData);
                }
            }
        }

        this.commitedFlg = true;
        
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();
    }

    public long getTransactionNo() {
        return transactionNo;
    }


    public void putTableData(TableData tableData) {
        Map<Long, TableData> modDataMap = this.modTableFolder.get(tableData.getParentTable().getTableName());
        if (modDataMap == null) {
            modDataMap = new HashMap();
            this.modTableFolder.put(tableData.getParentTable().getTableName(), modDataMap);
        }
        
        modDataMap.put(tableData.oid, tableData);
    }

    public String toString() {
        String ret = "";
        ret = ret + "TransactionNo:" + transactionNo;
        ret = ret + ", commitedFlg:" + commitedFlg;
        ret = ret + "modTableFolder:" + modTableFolder;
        return ret;
    }
}