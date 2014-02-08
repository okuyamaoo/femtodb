package femtodb.core.table.data;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;


public class TableData {

    private Object sync = new Object();
    public long oid = -1L;

    private TableInfo tableInfo = null;

    public TableDataTransfer newData = null; // ここは更新中データ、Rollbackデータ、commitデータが来る可能性がある。しかし同時に更新出来るトランザクションは1つのみ
    public TableDataTransfer oldData = null; // ここには必ずcommit済みのデータしかこない


    private ITable parentTable = null;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();


    public TableData(long oid, TransactionNo tn, TableInfo tableInfo, ITable parentTable, TableDataTransfer tableDataTransfer) {
        this.oid = oid;
        this.tableInfo = tableInfo;
        this.parentTable = parentTable;


        tableDataTransfer.setTransactionNo(tn);
        tableDataTransfer.saveTmpData();

        this.newData = tableDataTransfer;
        tn.putTableData(this);
    }

    public ITable getParentTable() {
        return this.parentTable;
    }

    public TableDataTransfer getNewData() {
        return newData;
    }

    public TableDataTransfer getOldData() {
        return oldData;
    }

/*    public void storeData(Map<Long, ColumnData> tableColumnData) {
        tableDataTransfer.storeData(oid, tableColumnData);
    }*/



    public TableDataTransfer getTableDataTransfer(TransactionNo targetTn) {
        
        readLock.lock();
        try {
            //System.out.println("newData.getTransactionNo().getTransactionNo()=" + newData.getTransactionNo().getTransactionNo());
            //System.out.println("targetTn.getTransactionNo()=" + targetTn.getTransactionNo());
            if (newData.getTransactionNo().getTransactionNo() == targetTn.getTransactionNo() && newData.getTransactionNo().isRollback() == false) {
                if (newData.isDeletedData()) {
                    return null;
                }
                return newData;
            }
    
            
            if (newData.getTransactionNo().isCommited() == true) {
                if (newData.isDeletedData()) {
                    return null;
                }
                return newData;
            }
    
            if (newData.getTransactionNo().isCommited() == false) {
                if (oldData == null) return null;
                if (oldData.isDeletedData()) {
                    return null;
                }
                return oldData;
            }
        } finally {
            readLock.unlock();
        }
        
        return null;

        /*
        int nowSize = this.dataList.size();
        ListIterator iter = dataList.listIterator(nowSize);

        while (iter.hasPrevious()) {

            TableDataTransfer tdt = (TableDataTransfer)iter.previous();

            if (tdt.getTransactionNo().getTransactionNo() == targetTn.getTransactionNo()) {
                if (tdt.isDeletedData()) {
                    return null;
                }
                return tdt;
            } else if (tdt.getTransactionNo().isCommited()) {
                if (tdt.getTransactionNo().getTransactionNo() < targetTn.getTransactionNo()) {
                    if (tdt.isDeletedData()) {
                        return null;
                    }
                    return tdt;
                }
                if (tdt.isDeletedData()) {
                    return null;
                }
                return tdt;
            }
        }
        return null;*/
    }

    public void modData(TransactionNo tn, TableDataTransfer tableDataTransfer) throws DuplicateUpdateException {
        writeLock.lock();
        try {

            if (newData.getTransactionNo().isRollback() == false && newData.getTransactionNo().isCommited() == false) {
                if (newData.getTransactionNo().getTransactionNo() == tn.getTransactionNo()) {
                    tableDataTransfer.setTransactionNo(tn);
                    tableDataTransfer.saveTmpData();
                    tn.putTableData(this);
                    newData = tableDataTransfer;
                    return;
                } else {
                    throw new DuplicateUpdateException("duplicate mod data");
                }
            }

            tableDataTransfer.setTransactionNo(tn);
            tableDataTransfer.saveTmpData();
            tn.putTableData(this);

            if (newData.getTransactionNo().isRollback() == true) {
                newData = tableDataTransfer;
            } else if (newData.getTransactionNo().isCommited() == true) {
                oldData = newData;
                newData = tableDataTransfer;
            }
        } finally {
            writeLock.unlock();
        }
    }


    public void deleteData(TransactionNo tn, TableDataTransfer tableDataTransfer) throws DuplicateDeleteException {
        writeLock.lock();
        try {

            if (newData.getTransactionNo().isRollback() == false && newData.getTransactionNo().isCommited() == false) {
                if (newData.getTransactionNo().getTransactionNo() == tn.getTransactionNo()) {
                    tableDataTransfer.setTransactionNo(tn);
                    tableDataTransfer.delete();
                    tn.putTableData(this);
                    newData = tableDataTransfer;
                    return;
                } else {
                    throw new DuplicateDeleteException("duplicate mod data");
                }
            }

            tableDataTransfer.setTransactionNo(tn);
            tableDataTransfer.delete();
            tn.putTableData(this);

            if (newData.getTransactionNo().isRollback() == true) {
                newData = tableDataTransfer;
            } else if (newData.getTransactionNo().isCommited() == true) {
                oldData = newData;
                newData = tableDataTransfer;
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    
    public int hashCode() {
        return new Long(oid).hashCode();
    }


    public boolean equals(Object targetData) {
        if (targetData instanceof TableData) {
            if (oid == ((TableData)targetData).oid) {
                return true;
            }
        }
        
        return false;
    }

}