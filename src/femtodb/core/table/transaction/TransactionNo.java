package femtodb.core.table.transaction;

import java.io.*;
import java.util.*;
import femtodb.core.table.index.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;


/** 
 * TransactionNoクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TransactionNo implements Serializable{

    private boolean commitedFlg = false;
    private boolean rollbackFlg = false;

    private long transactionNo = -1L;

    private Date createDate = new Date();

    public Map<String, Map<Long, TableData>> modTableFolder = null;

    private boolean execHalfCommit = false;

    public TransactionNo(long transactionNo) {
        this.transactionNo = transactionNo;
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();
    }

    public long TransactionNo() {
        return this.transactionNo;
    }

    public final String getDateString() {
        return createDate.toString();
    }

    public final boolean isCommited() {
        return this.commitedFlg;
    }

    public final boolean isRollback() {
        return this.rollbackFlg;
    }



    public void rollback() {
        // ここでmodTableFolderとmodDataMapと自身のtransactionNoを使ってTableDataの履歴データの掃除と
        for (String tableName : modTableFolder.keySet()) {
            Map<Long, TableData> modTableDataMap = modTableFolder.get(tableName);

            for (Iterator it = modTableDataMap.entrySet().iterator(); it.hasNext();) {
                Map.Entry entry = (Map.Entry)it.next();

                Long oid = (Long)entry.getKey();
                TableData tableData = (TableData)entry.getValue();
                if (tableData.oldData == null && tableData.newData != null && tableData.newData.getTransactionNo().getTransactionNo() == transactionNo) {
                    // 自身のトランザクション内で新規で作成したデータを削除する
                    // 実データはトランザクションで保護されているがデータ本体は存在しておりメモリを専有しているため
                    // rollbackで削除する
                    ITable defaultTable = tableData.getParentTable();
                    defaultTable.removeTmpData(oid);
                }
            }
        }

        this.rollbackFlg = true;
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();
    }

    /**
     * 2段階コミット処理の前半部分<br>
     * コミット時に時間がかかる処理を本処理では担当する<br>
     * この処理が完了した時点ではまだデータは同一トランザクション内からしか参照出来なくなっており<br>
     * 別トランザクションでは参照出来ない<br>
     *
     */
    public boolean halfCommit() {
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
        this.modTableFolder = new HashMap<String, Map<Long, TableData>>();

        execHalfCommit = true;
        return true;
    }


    /**
     * 2段階コミット処理の後半部分<br>
     * 同トランザクションのでの変更データが他のトランザクションからも参照可能にする処理となる<br>
     * ただし本処理を呼び出す前にhalfCommitが呼び出せていな場合はエラー<br>
     *
     */
    public boolean fixCommit() {
        if (!execHalfCommit) return false;

        this.commitedFlg = true;
        return true;
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

    public final long getTransactionNo() {
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