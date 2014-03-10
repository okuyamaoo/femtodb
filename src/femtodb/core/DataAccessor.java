package femtodb.core;


import java.util.*;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import femtodb.core.*;
import femtodb.core.util.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.data.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;



/** 
 * DataAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DataAccessor {

    private TableManager tableManager = null;
    private DataOperationLogManager dataOperationLogManager = null;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    private ReadWriteLock logLock = new ReentrantReadWriteLock(true);
    private Lock logWriteLock = logLock.writeLock();

    private RebuildTableDataWorker rebuildTableDataWorker = null;

    public DataAccessor() throws Exception {
        this(null);
    }

    public DataAccessor(String[] bootArgs) throws Exception {
        if (bootArgs != null) {
            FemtoDBConstants.build(bootArgs);
        }

        TransactionNoManager.initTransactionNoManager();
        TableManager.initOid();
        this.tableManager = new TableManager();
        // データ復元
        loadOperationLog();
    }

    public DataAccessor(String[] bootArgs, long setTransactionNo, long setOid) throws Exception {
        if (bootArgs != null) {
            FemtoDBConstants.build(bootArgs);
        }

        TransactionNoManager.initTransactionNoManager(setTransactionNo);
        TableManager.initOid(setOid);
        this.tableManager = new TableManager();
        // データ復元
        loadOperationLog();
    }

    // 操作ログよりデータを復元
    private void loadOperationLog() throws Exception {
        //QueryOptimizerへTableManagerを渡す
        QueryOptimizer.setTableManager(this.tableManager);

        // 復元処理中にログを出力するのは無意味なのでここでログ出力を抑制
        boolean writeConf = FemtoDBConstants.TRANSACTION_LOG_WRITE;
        if (writeConf == true) FemtoDBConstants.TRANSACTION_LOG_WRITE = false;

        // データをロード
        this.dataOperationLogManager = new DataOperationLogManager(FemtoDBConstants.TRANSACTION_LOG);
        this.dataOperationLogManager.loadOperationLog(this);

        // ログをもとに戻す
        if (writeConf == true) FemtoDBConstants.TRANSACTION_LOG_WRITE = true;

        List<TableInfo> list = this.tableManager.getTableInfoList();
        if (list != null && list.size() > 0) {
            for (TableInfo table : list) {
                rebuildIndex(table.tableName);
            }
        }
        rebuildTableDataWorker = new RebuildTableDataWorker(tableManager, this);
        rebuildTableDataWorker.start();
        // 復元後に1度だけGC呼び出し
        System.gc();
    }

    public TransactionNo createTransaction() {
        TransactionNo tn = TransactionNoManager.createTransactionNo();
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(1));
        } finally {
            logWriteLock.unlock();
            return tn;
        }
    }


    public boolean commitTransaction(long transactionNo) {
        TransactionNo tn = TransactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;
        return commitTransaction(tn);
    }

    public boolean commitTransaction(TransactionNo transactionNo) {
        boolean ret = TransactionNoManager.commitTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(2), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean rollbackTransaction(long transactionNo) {
        TransactionNo tn = TransactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;

        return rollbackTransaction(tn);
    }

    public boolean rollbackTransaction(TransactionNo transactionNo) {

        boolean ret = TransactionNoManager.rollbackTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(3), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean endTransaction(long transactionNo) {
        return endTransaction(TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public boolean endTransaction(TransactionNo transactionNo) {

        boolean ret = TransactionNoManager.endTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(4), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean existsTransactionNo(long transactionNo) {
        return TransactionNoManager.existsTransactionNoObejct(transactionNo);
    }


    public List<TransactionNo> getTransactionNoList() {
        return TransactionNoManager.getTransactionNoList();
    }


    public int createTable(TableInfo tableInfo) {

        TableAccessor tableDataAccessor = new TableAccessor(this.tableManager);

        int ret = tableDataAccessor.create(tableInfo);
        if (ret == 1) {
            logWriteLock.lock();
            try {
                tansactionLogWrite(Integer.valueOf(5), tableInfo);
//              SystemLog.println("o5=" + tableInfo.createStoreString());
            } finally {
                logWriteLock.unlock();
                return ret;
            }
        }
        return 2;
    }

    public TableInfo getTable(String tableName) {

        TableAccessor tableDataAccessor = new TableAccessor(this.tableManager);
        TableInfo ret = tableDataAccessor.get(tableName);
        return ret;
    }



    public List<TableInfo> getTableList() {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager);
        return tableAccessor.getTableList();
    }


    public TableInfo getTableInfo(String tableName) {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager);
        return tableAccessor.getTableInfo(tableName);
    }


    public TableInfo removeTable(String tableName) {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager);
        TableInfo tableInfo = tableAccessor.remove(tableName);
        if (tableInfo == null) return null;
        tansactionLogWrite(Integer.valueOf(99), tableName);
        return tableInfo;
    }



    public boolean rebuildIndex(String tableName) {

        TableAccessor tableAccessor = new TableAccessor(this.tableManager);
        return tableAccessor.rebuildIndex(tableName);
    }


    public boolean cleanDeletedData(String tableName) {

        TableAccessor tableAccessor = new TableAccessor(this.tableManager);
        return tableAccessor.cleanDeletedData(tableName);
    }


    public int insertTableData(String tableName, long transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        return insertTableData(tableName, TransactionNoManager.getTransactionNoObejct(transactionNo), tableDataTransfer, null);
    }


    public int insertTableData(String tableName, long transactionNo, TableDataTransfer tableDataTransfer, String uniqueKey) throws InsertException {
        return insertTableData(tableName, TransactionNoManager.getTransactionNoObejct(transactionNo), tableDataTransfer, uniqueKey);
    }

    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        return insertTableData(tableName, transactionNo, tableDataTransfer, null);
    }
    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer, String uniqueKey) throws InsertException {
        InsertTableAccessor insertTableAccessor = new InsertTableAccessor(this.tableManager);
        try {
            int ret = -1;
            if (uniqueKey == null) {
                ret = insertTableAccessor.insert(tableName, transactionNo, tableDataTransfer);
            } else {
                ret = insertTableAccessor.insert(tableName, transactionNo, tableDataTransfer, uniqueKey);
            }
            logWriteLock.lock();
            try {
                tansactionLogWrite(Integer.valueOf(6), tableName, transactionNo, tableDataTransfer, uniqueKey);

                // データの登録のQptimizerへ登録
                QueryOptimizer.lastUpdateAccessTimeInfo.put(tableName, System.nanoTime());
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } catch (Exception e) {
            throw new InsertException(e);
        }
    }

    public List<TableDataTransfer> selectTableDataList(SelectParameter selectParameter, long transactionNo) throws SelectException {
        ResultStruct resultStruct = selectTableData(selectParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
        return resultStruct.getResultList();
    }

    public List<TableDataTransfer> selectTableDataList(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {
        ResultStruct resultStruct = selectTableData(selectParameter, transactionNo);
        return resultStruct.getResultList();
    }

    public ResultStruct selectTableData(SelectParameter selectParameter, long transactionNo) throws SelectException {
        return selectTableData(selectParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public ResultStruct selectTableData(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {
        SelectTableAccessor selectTableAccessor = new SelectTableAccessor(this.tableManager);
    
        ResultStruct resultStruct = null;
        resultStruct = selectTableAccessor.select(selectParameter, transactionNo);

        return resultStruct;
    }


    public int updateTableData(UpdateParameter updateParameter, long transactionNo) throws UpdateException {
        return updateTableData(updateParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int updateTableData(UpdateParameter updateParameter, TransactionNo transactionNo) throws UpdateException {
        UpdateTableAccessor updateTableAccessor = new UpdateTableAccessor(this.tableManager);
        try {
            QueryOptimizer.lastUpdateAccessTimeInfo.put(updateParameter.getTableName(), System.nanoTime());
            int ret = updateTableAccessor.update(updateParameter, transactionNo);

            logWriteLock.lock();
            try {
                tansactionLogWrite(Integer.valueOf(7), updateParameter, transactionNo);
                
            } finally {
                logWriteLock.unlock();
                return ret;
            }
        } catch (DuplicateUpdateException due) {
            this.rollbackTransaction(transactionNo);
            throw new UpdateException("Duplicate update !! Auto rollback executed", due);
        } catch (Exception e) {
            this.rollbackTransaction(transactionNo);
            throw new UpdateException("Unknow Exception !! Auto rollback executed", e);
        }
    }

    public int deleteTableData(DeleteParameter deleteParameter, long transactionNo) throws DeleteException {
        return deleteTableData(deleteParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int deleteTableData(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DeleteException {
        DeleteTableAccessor deleteTableAccessor = new DeleteTableAccessor(this.tableManager);
        try {
            QueryOptimizer.lastUpdateAccessTimeInfo.put(deleteParameter.getTableName(), System.nanoTime());
            int ret = deleteTableAccessor.delete(deleteParameter, transactionNo);

            logWriteLock.lock();
            try {
                tansactionLogWrite(Integer.valueOf(8), deleteParameter, transactionNo);
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } catch (DuplicateDeleteException due) {
            this.rollbackTransaction(transactionNo);
            throw new DeleteException("Duplicate delete data !! Auto rollback executed", due);
        } catch (Exception e) {
            this.rollbackTransaction(transactionNo);
            throw new DeleteException("Unknow Exception !! Auto rollback executed", e);
        }
    }


    // 操作ログ出力
    private boolean tansactionLogWrite(Object... logObjects) {
        if (FemtoDBConstants.TRANSACTION_LOG_WRITE) {

            return this.dataOperationLogManager.operationLogWrite(logObjects);
        }
        return false;
    }

    class RebuildTableDataWorker extends Thread {
        private TableManager tableManager = null;
        private DataAccessor baseInstance = null;

        RebuildTableDataWorker(TableManager tableManager, DataAccessor accessor) {
            this.tableManager = tableManager;
            this.baseInstance = accessor;
        }

        public void run() {
            try {
                this.setPriority(1);
                while (true) {
                    try {
                        List<TableInfo> list = tableManager.getTableInfoList();
                        for (TableInfo info : list) {
                            this.baseInstance.rebuildIndex(info.tableName);
                            Thread.sleep(3000);
                        }
                        Thread.sleep(7000);
                        for (TableInfo info : list) {
                            this.baseInstance.cleanDeletedData(info.tableName);
                            Thread.sleep(3000);
                        }
                        Thread.sleep(7000);
                    } catch (Exception e) {
                    }
                    
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}