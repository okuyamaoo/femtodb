package femtodb.core.table.data;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import femtodb.core.*;
import femtodb.core.util.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;


/** 
 * TableDataクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TableData implements Serializable {

    public long oid = -1L;
    public int hashCode = -1;


    private String uniqueKey = null;


    public TableDataTransfer newData = null; // ここは更新中データ、Rollbackデータ、commitデータが来る可能性がある。しかし同時に更新出来るトランザクションは1つのみ
    public TransactionNo newTransactionNo = null; 

    public TableDataTransfer oldData = null; // ここには必ずcommit済みのデータしかこない
    public TransactionNo oldTransactionNo = null;

    private ITable parentTable = null;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();


    public TableData(long oid, TransactionNo tn, ITable parentTable, TableDataTransfer tableDataTransfer) {
        this.oid = oid;
        this.hashCode = Long.valueOf(oid).hashCode();
        this.parentTable = parentTable;


        tableDataTransfer.setTransactionNo(tn);
        tableDataTransfer.saveTmpData();

        this.newData = tableDataTransfer;
        this.newTransactionNo = this.newData.getTransactionNo();
        tn.putTableData(this);
    }


    public TableData(long oid, TransactionNo tn, ITable parentTable, TableDataTransfer tableDataTransfer, String uniqueKey) {
        this.oid = oid;
        this.hashCode = Long.valueOf(oid).hashCode();
        this.uniqueKey = uniqueKey;
        this.parentTable = parentTable;

        tableDataTransfer.setTransactionNo(tn);
        tableDataTransfer.saveTmpData();

        this.newData = tableDataTransfer;
        this.newTransactionNo = this.newData.getTransactionNo();

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

    public String getUniqueKey() {
        return this.uniqueKey;
    }

/*    public void storeData(Map<Long, ColumnData> tableColumnData) {
        tableDataTransfer.storeData(oid, tableColumnData);
    }*/



    public final TableDataTransfer getTableDataTransfer(TransactionNo targetTn) {
        // 匿名セッション判定
        if (targetTn.getTransactionNo() == Long.MAX_VALUE) {
            // 匿名セッションである
            TableDataTransfer retData = newData;
            if (retData.getTransactionNo().isCommited() == true) {
                if (retData.isDeletedData()) {
                    return null;
                }
                return retData;
            }
        }

        readLock.lock();
        try {

            //SystemLog.println("newData.getTransactionNo().getTransactionNo()=" + newData.getTransactionNo().getTransactionNo());
            //SystemLog.println("targetTn.getTransactionNo()=" + targetTn.getTransactionNo());
            if (this.newTransactionNo.isCommited() == true) {
                if (newData.isDeletedData()) {
                    return null;
                }
                return newData;
            }

            if (this.newTransactionNo.getTransactionNo() == targetTn.getTransactionNo() && this.newTransactionNo.isRollback() == false) {
                if (newData.isDeletedData()) {
                    return null;
                }
                return newData;
            }

            if (this.newTransactionNo.isCommited() == false) {
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
    }

    public void modData(TransactionNo tn, TableDataTransfer tableDataTransfer) throws DuplicateUpdateException {
        writeLock.lock();
        try {

            if (this.newTransactionNo.isRollback() == false && this.newTransactionNo.isCommited() == false) {
                if (this.newTransactionNo.getTransactionNo() == tn.getTransactionNo()) {
                    tableDataTransfer.setTransactionNo(tn);
                    tableDataTransfer.saveTmpData();
                    tn.putTableData(this);
                    this.newData = tableDataTransfer;
                    this.newTransactionNo = this.newData.getTransactionNo();

                    return;
                } else {
                    throw new DuplicateUpdateException("duplicate mod data");
                }
            }

            tableDataTransfer.setTransactionNo(tn);
            tableDataTransfer.saveTmpData();
            tn.putTableData(this);

            if (this.newTransactionNo.isRollback() == true) {
                this.newData = tableDataTransfer;
                this.newTransactionNo = this.newData.getTransactionNo();

            } else if (this.newTransactionNo.isCommited() == true) {
                this.oldData = newData;
                this.oldTransactionNo = this.oldData.getTransactionNo();
                
                this.newData = tableDataTransfer;
                this.newTransactionNo = this.newData.getTransactionNo();
            }
        } finally {
            writeLock.unlock();
        }
    }


    public void deleteData(TransactionNo tn, TableDataTransfer tableDataTransfer) throws DuplicateDeleteException {
        writeLock.lock();
        try {

            if (this.newTransactionNo.isRollback() == false && this.newTransactionNo.isCommited() == false) {
                if (this.newTransactionNo.getTransactionNo() == tn.getTransactionNo()) {
                    tableDataTransfer.setTransactionNo(tn);
                    tableDataTransfer.delete();
                    tn.putTableData(this);
                    this.newData = tableDataTransfer;
                    this.newTransactionNo = this.newData.getTransactionNo();

                    return;
                } else {
                    throw new DuplicateDeleteException("duplicate mod data");
                }
            }

            tableDataTransfer.setTransactionNo(tn);
            tableDataTransfer.delete();
            tn.putTableData(this);

            if (this.newTransactionNo.isRollback() == true) {
                this.newData = tableDataTransfer;
                this.newTransactionNo = this.newData.getTransactionNo();

            } else if (this.newTransactionNo.isCommited() == true) {
                this.oldData = this.newData;
                this.oldTransactionNo = this.oldData.getTransactionNo();

                this.newData = tableDataTransfer;
                this.newTransactionNo = this.newData.getTransactionNo();
            }
        } finally {
            writeLock.unlock();
        }
    }
    
    
    public int hashCode() {
        return this.hashCode;
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