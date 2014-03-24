package femtodb.core;


import java.io.*;
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
    private QueryOptimizer queryOptimizer = QueryOptimizer.getNewInstance();
    private TransactionNoManager transactionNoManager = TransactionNoManager.getNewInstance();
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
        this(bootArgs, false);
    }
    
    public DataAccessor(String[] bootArgs, boolean backUpProccess) throws Exception {

        if (backUpProccess == false && bootArgs != null) {
            FemtoDBConstants.build(bootArgs);
        }

        File objFile = new File(FemtoDBConstants.TRANSACTION_LOG + ".obj");
        if (objFile.exists()) {
            StoreTableManagerFolder storeTableManagerFolder = null;
            FileInputStream inFile = null;
            ObjectInputStream inObject = null;
            try {
                inFile = new FileInputStream(objFile); 
                inObject = new ObjectInputStream(inFile);

                storeTableManagerFolder = (StoreTableManagerFolder)inObject.readObject();
                inObject.close();
                inFile.close();
            } catch (Exception e) {
                e.printStackTrace();
                try {
                    storeTableManagerFolder = null;
                    if (inObject == null) inObject.close();
                    if (inFile == null) inFile.close();
                } catch (Exception e2) {}

                try {
                    File oldObjFile = new File(FemtoDBConstants.TRANSACTION_LOG + ".obj.old");
                    if (oldObjFile.exists()) {
                        inFile = new FileInputStream(oldObjFile);
                        inObject = new ObjectInputStream(inFile);
                        storeTableManagerFolder = (StoreTableManagerFolder)inObject.readObject();
                        inObject.close();
                        inFile.close();
                    }
                } catch (Exception e2) {
                    storeTableManagerFolder = null;
                }
            }
            
            if (storeTableManagerFolder != null) {
                transactionNoManager.initTransactionNoManager(storeTableManagerFolder.getTransactionNoObj(), storeTableManagerFolder.getTransactionStatusMapObj());
    
                this.tableManager = storeTableManagerFolder.getTableManager();
                List<TableInfo> list = this.tableManager.getTableInfoList();

                // データ復元
                loadOperationLog(storeTableManagerFolder.getStoredLogNo());
                if (list != null && list.size() > 0) {
                    for (TableInfo table : list) {
                        this.createAllDataIndex(table.tableName);
                    }
                }
            } else {
                // 通常の復元シーケンス
                transactionNoManager.initTransactionNoManager();
                TableManager.initOid();
                this.tableManager = new TableManager();
                // データ復元
                loadOperationLog(-1L);
            }
        } else {
            transactionNoManager.initTransactionNoManager();
            TableManager.initOid();
            this.tableManager = new TableManager();
            // データ復元
            loadOperationLog(-1L);
        }
        
    }


    // 操作ログよりデータを復元
    private void loadOperationLog(long readLogNo) throws Exception {

        //QueryOptimizerへTableManagerを渡す
        queryOptimizer.setTableManager(this.tableManager);

        // 復元処理中にログを出力するのは無意味なのでここでログ出力を抑制
        boolean writeConf = FemtoDBConstants.TRANSACTION_LOG_WRITE;
        if (writeConf == true) FemtoDBConstants.TRANSACTION_LOG_WRITE = false;

        // データをロード
        this.dataOperationLogManager = new DataOperationLogManager(FemtoDBConstants.TRANSACTION_LOG);
        this.dataOperationLogManager.loadOperationLog(this, readLogNo);

        // ログをもとに戻す
        if (writeConf == true) FemtoDBConstants.TRANSACTION_LOG_WRITE = true;

        List<TableInfo> list = this.tableManager.getTableInfoList();
        if (list != null && list.size() > 0) {
            for (TableInfo table : list) {
                rebuildIndex(table.tableName);
                createAllDataIndex(table.tableName);
            }
        }
        rebuildTableDataWorker = new RebuildTableDataWorker(tableManager, this);
        rebuildTableDataWorker.start();
        // 復元後に1度だけGC呼び出し
        System.gc();
    }


    public TransactionNo createAnonymousTransaction() {
        TransactionNo tn = transactionNoManager.createAnonymousTransactionNo();
        return tn;
    }

    public TransactionNo createTransaction() {
        TransactionNo tn = transactionNoManager.createTransactionNo();
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(1));
        } finally {
            logWriteLock.unlock();
            return tn;
        }
    }


    public boolean commitTransaction(long transactionNo) {
        TransactionNo tn = transactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;
        return commitTransaction(tn);
    }

    public boolean commitTransaction(TransactionNo transactionNo) {
        boolean ret = transactionNoManager.commitTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(2), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean halfCommitTransaction(long transactionNo) {
        TransactionNo tn = transactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;
        return halfCommitTransaction(tn);
    }

    public boolean halfCommitTransaction(TransactionNo transactionNo) {
        boolean ret = transactionNoManager.halfCommitTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(211), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }


    public boolean fixCommitTransaction(long transactionNo) {
        TransactionNo tn = transactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;
        return fixCommitTransaction(tn);
    }

    public boolean fixCommitTransaction(TransactionNo transactionNo) {
        boolean ret = transactionNoManager.fixCommitTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(212), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }


    public boolean rollbackTransaction(long transactionNo) {
        TransactionNo tn = transactionNoManager.getTransactionNoObejct(transactionNo);
        if (tn == null) return false;

        return rollbackTransaction(tn);
    }

    public boolean rollbackTransaction(TransactionNo transactionNo) {

        boolean ret = transactionNoManager.rollbackTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(3), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean endTransaction(long transactionNo) {
        return endTransaction(transactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public boolean endTransaction(TransactionNo transactionNo) {

        boolean ret = transactionNoManager.endTransaction(transactionNo);
        logWriteLock.lock();
        try {
            tansactionLogWrite(Integer.valueOf(4), transactionNo);
        } finally {
            logWriteLock.unlock();
            return ret;
        }
    }

    public boolean existsTransactionNo(long transactionNo) {
        return transactionNoManager.existsTransactionNoObejct(transactionNo);
    }


    public List<TransactionNo> getTransactionNoList() {
        return transactionNoManager.getTransactionNoList();
    }


    public int createTable(TableInfo tableInfo) {

        TableAccessor tableDataAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);

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

        TableAccessor tableDataAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        TableInfo ret = tableDataAccessor.get(tableName);
        return ret;
    }



    public List<TableInfo> getTableList() {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        return tableAccessor.getTableList();
    }


    public TableInfo getTableInfo(String tableName) {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        return tableAccessor.getTableInfo(tableName);
    }


    public TableInfo removeTable(String tableName) {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        TableInfo tableInfo = tableAccessor.remove(tableName);
        if (tableInfo == null) return null;
        tansactionLogWrite(Integer.valueOf(99), tableName);
        return tableInfo;
    }


    public boolean addIndexColumn(TableInfo tableInfo) {
        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        tableAccessor.addIndexColumn(tableInfo);

        return true;
    }

    public boolean createAllDataIndex(String tableName) {
        // 本メソッドで作成されるトランザクションは全てAnonymousTransactionNoとなる。
        // See:TableAccessor.java     public boolean createAllDataIndex(String tableName) 
        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        return tableAccessor.createAllDataIndex(tableName);
    }

    public boolean rebuildIndex(String tableName) {

        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        return tableAccessor.rebuildIndex(tableName);
    }


    public boolean cleanDeletedData(String tableName) {

        TableAccessor tableAccessor = new TableAccessor(this.tableManager, queryOptimizer, transactionNoManager);
        return tableAccessor.cleanDeletedData(tableName);
    }


    public int insertTableData(String tableName, long transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        return insertTableData(tableName, transactionNoManager.getTransactionNoObejct(transactionNo), tableDataTransfer, null);
    }


    public int insertTableData(String tableName, long transactionNo, TableDataTransfer tableDataTransfer, String uniqueKey) throws InsertException {
        return insertTableData(tableName, transactionNoManager.getTransactionNoObejct(transactionNo), tableDataTransfer, uniqueKey);
    }

    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        return insertTableData(tableName, transactionNo, tableDataTransfer, null);
    }
    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer, String uniqueKey) throws InsertException {
        InsertTableAccessor insertTableAccessor = new InsertTableAccessor(this.tableManager, queryOptimizer);
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
                queryOptimizer.lastUpdateAccessTimeInfo.put(tableName, System.nanoTime());
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } catch (Exception e) {
            throw new InsertException(e);
        }
    }

    public TableDataTransfer getTableData(String tableName, String uniqueKey, long transactionNo) {
        return getTableData(tableName, uniqueKey, transactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public TableDataTransfer getTableData(String tableName, String uniqueKey, TransactionNo transactionNo) {
        ITable table = this.tableManager.getTableData(tableName);
        if (table == null) return null;
        return table.getTableData4UniqueKey(transactionNo, uniqueKey);
    }

    public List<TableDataTransfer> selectTableDataList(SelectParameter selectParameter, long transactionNo) throws SelectException {
        ResultStruct resultStruct = selectTableData(selectParameter, transactionNoManager.getTransactionNoObejct(transactionNo));
        return resultStruct.getResultList();
    }

    public List<TableDataTransfer> selectTableDataList(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {
        ResultStruct resultStruct = selectTableData(selectParameter, transactionNo);
        return resultStruct.getResultList();
    }

    public ResultStruct selectTableData(SelectParameter selectParameter, long transactionNo) throws SelectException {
        return selectTableData(selectParameter, transactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public ResultStruct selectTableData(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {
        SelectTableAccessor selectTableAccessor = new SelectTableAccessor(this.tableManager, queryOptimizer);
    
        ResultStruct resultStruct = null;
        resultStruct = selectTableAccessor.select(selectParameter, transactionNo);

        return resultStruct;
    }


    public int updateTableData(UpdateParameter updateParameter, long transactionNo) throws UpdateException {
        return updateTableData(updateParameter, transactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int updateTableData(UpdateParameter updateParameter, TransactionNo transactionNo) throws UpdateException {
        UpdateTableAccessor updateTableAccessor = new UpdateTableAccessor(this.tableManager, queryOptimizer);
        try {
            queryOptimizer.lastUpdateAccessTimeInfo.put(updateParameter.getTableName(), System.nanoTime());
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
        return deleteTableData(deleteParameter, transactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int deleteTableData(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DeleteException {

        DeleteTableAccessor deleteTableAccessor = new DeleteTableAccessor(this.tableManager, queryOptimizer);
        try {
            queryOptimizer.lastUpdateAccessTimeInfo.put(deleteParameter.getTableName(), System.nanoTime());
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
            e.printStackTrace();
            this.rollbackTransaction(transactionNo);
            throw new DeleteException("Unknow Exception !! Auto rollback executed", e);
        }
    }

    public boolean storeTableObject() {
        logWriteLock.lock();
        try {
            StoreTableManagerFolder folder = new StoreTableManagerFolder(this.dataOperationLogManager.getLogNo());

            folder.setTableManager(this.tableManager);
            folder.setTransactionNoObj(transactionNoManager.getNoObject());
            folder.setTransactionStatusMapObj(transactionNoManager.getStatusMapObject());

            /*File nowObjFile = new File(FemtoDBConstants.TRANSACTION_LOG + ".obj");
            File changeFile = null;
            if (nowObjFile.exists()) {
                changeFile = new File(FemtoDBConstants.TRANSACTION_LOG + ".obj.old");
                nowObjFile.renameTo(changeFile);
            }*/

            FileOutputStream objFile = new FileOutputStream(FemtoDBConstants.TRANSACTION_LOG + ".obj");
            ObjectOutputStream outObj = new ObjectOutputStream(objFile);
            outObj.writeObject(folder);
            outObj.close();
            objFile.close();
            //if (changeFile != null) changeFile.delete();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            logWriteLock.unlock();
        }
        return true;
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
                            Thread.sleep(10000);
                        }

                        Thread.sleep(7000);
                        for (TableInfo info : list) {
                            this.baseInstance.cleanDeletedData(info.tableName);
                            Thread.sleep(3000);
                        }
                        Thread.sleep(7000);

                        // TODO:自動バックアップは一時的に停止
                        /*System.out.println("start");
                        long objStoreStart = System.nanoTime();
                        if (!this.baseInstance.storeTableObject()) System.out.println("error");
                        long objStoreEnd = System.nanoTime();
                        System.out.println(" Stored time" + ((objStoreEnd - objStoreStart) / 1000 /1000) + "ms");
                        System.out.println("end");*/
                    } catch (Throwable e) {
                        e.printStackTrace();
                    }
                    
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}