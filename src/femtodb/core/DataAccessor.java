package femtodb.core;


import java.util.*;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.data.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;



public class DataAccessor {

    private TableManager tableManager = null;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();

    private ReadWriteLock logLock = new ReentrantReadWriteLock(true);
    private Lock logWriteLock = logLock.writeLock();


    public DataAccessor() {
        TransactionNoManager.initTransactionNoManager();
        TableManager.initOid();
        this.tableManager = new TableManager();
    }

    public DataAccessor(long setTransactionNo, long setOid) {
        TransactionNoManager.initTransactionNoManager(setTransactionNo);
        TableManager.initOid(setOid);
        
        this.tableManager = new TableManager();
    }

    public TransactionNo createTransaction() {
        readLock.lock();
        try {
            TransactionNo tn = TransactionNoManager.createTransactionNo();
            logWriteLock.lock();
            try {
                tansactionLogWrite("o1");
            } finally {
                logWriteLock.unlock();
                return tn;
            }
        } finally {
            readLock.unlock();
        }
    }


    public boolean commitTransaction(long transactionNo) {
        return commitTransaction(TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public boolean commitTransaction(TransactionNo transactionNo) {
        readLock.lock();
        try {
            boolean ret = TransactionNoManager.commitTransaction(transactionNo);
            logWriteLock.lock();
            try {
                tansactionLogWrite("o2=" + transactionNo.getTransactionNo());
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } finally {
            readLock.unlock();
        }
    }

    public boolean rollbackTransaction(long transactionNo) {
        return rollbackTransaction(TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public boolean rollbackTransaction(TransactionNo transactionNo) {
        readLock.lock();
        try {

            boolean ret = TransactionNoManager.rollbackTransaction(transactionNo);
            logWriteLock.lock();
            try {
                tansactionLogWrite("o3=" + transactionNo.getTransactionNo());
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } finally {
            readLock.unlock();
        }
    }

    public boolean endTransaction(long transactionNo) {
        return endTransaction(TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public boolean endTransaction(TransactionNo transactionNo) {
        readLock.lock();
        try {

            boolean ret = TransactionNoManager.endTransaction(transactionNo);
            logWriteLock.lock();
            try {
                tansactionLogWrite("o4=" + transactionNo.getTransactionNo());
            } finally {
                logWriteLock.unlock();
                return ret;
            }

        } finally {
            readLock.unlock();
        }
    }

    public int createTable(TableInfo tableInfo) {
        readLock.lock();
        try {

            TableAccessor tableDataAccessor = new TableAccessor(this.tableManager);
            int ret = tableDataAccessor.create(tableInfo);
            logWriteLock.lock();
            try {
                //tansactionLogWrite("o5=" + tableInfo.createStoreString());
                System.out.println("o5=" + tableInfo.createStoreString());
            } finally {
                logWriteLock.unlock();
                return ret;
            }
        } finally {
            readLock.unlock();
        }
    }

    public TableInfo getTable(String tableName) {
        readLock.lock();
        try {

            TableAccessor tableDataAccessor = new TableAccessor(this.tableManager);
            TableInfo ret = tableDataAccessor.get(tableName);
            return ret;
        } finally {
            readLock.unlock();
        }
    }


    public TableInfo getTableInfo(String tableName) {
        readLock.lock();
        try {

            TableAccessor tableAccessor = new TableAccessor(this.tableManager);
            return tableAccessor.getTableInfo(tableName);
        } finally {
            readLock.unlock();
        }
    }


    public boolean rebuildIndex(String tableName) {
        readLock.lock();
        try {

            TableAccessor tableAccessor = new TableAccessor(this.tableManager);
            return tableAccessor.rebuildIndex(tableName);
        } finally {
            readLock.unlock();
        }
    }


    public int insertTableData(String tableName, long transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        return insertTableData(tableName, TransactionNoManager.getTransactionNoObejct(transactionNo), tableDataTransfer);
    }

    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        readLock.lock();
        try {

            InsertTableAccessor insertTableAccessor = new InsertTableAccessor(this.tableManager);
            try {

                int ret = insertTableAccessor.insert(tableName, transactionNo, tableDataTransfer);
                logWriteLock.lock();
                try {
                    tansactionLogWrite("o6=" + tableName + "\t" + transactionNo.getTransactionNo() + "\t" + tableDataTransfer.toString());
                } finally {
                    logWriteLock.unlock();
                    return ret;
                }

            } catch (Exception e) {
                throw new InsertException(e);
            }
        } finally {
            readLock.unlock();
        }
    }

    public List<TableDataTransfer> selectTableData(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {
        readLock.lock();
        try {
            SelectTableAccessor selectTableAccessor = new SelectTableAccessor(this.tableManager);
            try {
                List retList = selectTableAccessor.select(selectParameter, transactionNo);
                return retList;
            } catch (Exception e) {
                throw new SelectException(e);
            }
        } finally {
            readLock.unlock();
        }
    }


    public int updateTableData(UpdateParameter updateParameter, long transactionNo) throws UpdateException {
        return updateTableData(updateParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int updateTableData(UpdateParameter updateParameter, TransactionNo transactionNo) throws UpdateException {
        readLock.lock();
        try {
            UpdateTableAccessor updateTableAccessor = new UpdateTableAccessor(this.tableManager);
            try {
                int ret = updateTableAccessor.update(updateParameter, transactionNo);
                logWriteLock.lock();
                try {
                    tansactionLogWrite("o7=" + updateParameter.toString() + "\t" + transactionNo.getTransactionNo());
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
        } finally {
            readLock.unlock();
        }
    }

    public int deleteTableData(DeleteParameter deleteParameter, long transactionNo) throws DeleteException {
        return deleteTableData(deleteParameter, TransactionNoManager.getTransactionNoObejct(transactionNo));
    }

    public int deleteTableData(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DeleteException {
        readLock.lock();
        try {
            DeleteTableAccessor deleteTableAccessor = new DeleteTableAccessor(this.tableManager);
            try {
                int ret = deleteTableAccessor.delete(deleteParameter, transactionNo);
                logWriteLock.lock();
                try {
                    tansactionLogWrite("o8=" + deleteParameter.toString() + "\t" + transactionNo.getTransactionNo());
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
        } finally {
            readLock.unlock();
        }
    }

    private boolean tansactionLogWrite(String logLine) {
        return true;
    }
}