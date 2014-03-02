package femtodb.core.accessor;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;



/** 
 * DeleteTableAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DeleteTableAccessor {


    TableManager tableManager = null;

    public DeleteTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public int delete(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DuplicateDeleteException, Exception {

        try {
            if (deleteParameter.existNormalWhereParameter() || deleteParameter.existIndexWhereParameter()) {
                
                return delete(deleteParameter.getTableName(), deleteParameter, transactionNo);
            } else {
                // 全件削除
                return deleteAll(deleteParameter.getTableName(), deleteParameter, transactionNo);
            }
        } catch (DuplicateDeleteException duplicateDeleteException) {
            throw duplicateDeleteException;
        } catch (Exception e) {
            throw e;
        }
    }

    /**
     * 全件更新
     *
     *
     */
    protected int deleteAll(String tableName, DeleteParameter deleteParameter, TransactionNo transactionNo) throws DuplicateDeleteException {
        ITable table = this.tableManager.getTableData(tableName);
        int retCount = 0;

        TableIterator iterator = table.getTableDataIterator();


        for (; iterator.hasNext();) {

            iterator.nextEntry();
            TableData tableData = (TableData)iterator.getEntryValue();
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
            if (tableDataTransfer == null) continue; // Delete済みのTableDataの場合はTableTransaferはnull
            TableDataTransfer deleteTableDataTransfer = tableDataTransfer.clone4Delete();

            try {
                tableData.deleteData(transactionNo, deleteTableDataTransfer);
                retCount++;
            } catch (DuplicateDeleteException duplicateDeleteException) {
                throw duplicateDeleteException;
            }
        }
        return retCount;
    }


    /**
     * 条件適応
     *
     *
     */
    protected int delete(String tableName, DeleteParameter deleteParameter, TransactionNo transactionNo) throws Exception, DuplicateDeleteException {
        Thread th = Thread.currentThread();
        try {
            ITable table = this.tableManager.getTableData(tableName);
            int retCount = 0;
    
            List<Object[]> allData = new ArrayList<Object[]>(table.getRecodeSize());

            // Optimizerにスレッド制御を実施させる
            Object syncObj = QueryOptimizer.getParallelsSyncObject(transactionNo, (SelectParameter)deleteParameter, th);
            // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
            TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, deleteParameter, table);

            synchronized(syncObj) {
                NormalWhereParameter normalWhereParameter = deleteParameter.nextNormalWhereParameter();
                NormalWhereExecutor normalWhereExecutor = null;
                if (normalWhereParameter != null) {
                    try {
                        normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
                    } catch (Exception e) {
                        throw e;
                    }
                }
        
                for (; iterator.hasNext();) {
        
                    iterator.nextEntry();
                    TableData tableData = (TableData)iterator.getEntryValue();
                    TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
                    if (tableDataTransfer != null) {
                        if (normalWhereParameter != null) {
                            if (normalWhereExecutor.execute(tableDataTransfer)) {
        
                                Object[] data = new Object[2];
                                data[0] = tableData;
                                data[1] = tableDataTransfer;
                                allData.add(data);
                            }
                        } else {
                            Object[] data = new Object[2];
                            data[0] = tableData;
                            data[1] = tableDataTransfer;
                            allData.add(data);
                        }
                    }
                }
        
                while ((normalWhereParameter = deleteParameter.nextNormalWhereParameter()) != null) {
                    normalWhereExecutor = null;
                    normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
                    if (allData.size() > 0) {
                        int size = allData.size();
                        List<Object[]> tmpAllData = new ArrayList<Object[]>(size);
        
                        for (int i = 0; i < size; i++) {
                            Object[] data = allData.get(i);
                            
                            TableDataTransfer targetTableDataTransfer = (TableDataTransfer)data[1];
                            if (normalWhereExecutor.execute(targetTableDataTransfer)) {
                                tmpAllData.add(data);
                            }
                        }
                        allData = tmpAllData;
                    }
                }
    
                int deleteTargetDataSize = allData.size();
        
                for (int i = 0; i < deleteTargetDataSize; i++) {
        
                    Object[] data = allData.get(i);
                    TableData tableData = (TableData)data[0];
                    TableDataTransfer tableDataTransfer = (TableDataTransfer)data[1];
                    TableDataTransfer deleteTableDataTransfer = tableDataTransfer.clone4Delete();
        
                    try {
                        tableData.deleteData(transactionNo, deleteTableDataTransfer);
                        retCount++;
                    } catch (DuplicateDeleteException duplicateDeleteException) {
                        throw duplicateDeleteException;
                    }
        
                }
            }
            allData = null;
            return retCount;
        } catch (Exception e) {
            throw e;
        } finally {
            QueryOptimizer.removeThreadGroupData(th);
        }
    }

}