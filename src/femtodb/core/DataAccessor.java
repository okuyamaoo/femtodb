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
        
            return TransactionNoManager.createTransactionNo();
        } finally {
            readLock.unlock();
        }
    }

    public boolean commitTransaction(TransactionNo transactionNo) {
        readLock.lock();
        try {

            return TransactionNoManager.commitTransaction(transactionNo);
        } finally {
            readLock.unlock();
        }
    }

    public boolean rollbackTransaction(TransactionNo transactionNo) {
        readLock.lock();
        try {

            return TransactionNoManager.rollbackTransaction(transactionNo);
        } finally {
            readLock.unlock();
        }
    }

    public int createTable(TableInfo tableInfo) {
        readLock.lock();
        try {

            TableAccessor tableDataAccessor = new TableAccessor(this.tableManager);
            int ret = tableDataAccessor.create(tableInfo);
            return ret;
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


    public int insertTableData(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer) throws InsertException {
        readLock.lock();
        try {

            InsertTableAccessor insertTableAccessor = new InsertTableAccessor(this.tableManager);
            try {

                return insertTableAccessor.insert(tableName, transactionNo, tableDataTransfer);
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


    public int updateTableData(UpdateParameter updateParameter, TransactionNo transactionNo) throws UpdateException {
        readLock.lock();
        try {
            UpdateTableAccessor updateTableAccessor = new UpdateTableAccessor(this.tableManager);
            try {
                int retCount = updateTableAccessor.update(updateParameter, transactionNo);
                return retCount;
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


    public int deleteTableData(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DeleteException {
        readLock.lock();
        try {
            DeleteTableAccessor deleteTableAccessor = new DeleteTableAccessor(this.tableManager);
            try {
                int retCount = deleteTableAccessor.delete(deleteParameter, transactionNo);
                return retCount;
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

}