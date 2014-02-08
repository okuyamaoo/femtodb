package femtodb.core.accessor;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;



public class DeleteTableAccessor {


    TableManager tableManager = null;

    public DeleteTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public int delete(DeleteParameter deleteParameter, TransactionNo transactionNo) throws DuplicateUpdateException, Exception {

        if (deleteParameter.existNormalWhereParameter() || deleteParameter.existIndexWhereParameter()) {
            
            return delete(deleteParameter.getTableName(), deleteParameter, transactionNo);
        } else {
            // 全件削除
            return deleteAll(deleteParameter.getTableName(), deleteParameter, transactionNo);
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
    protected int delete(String tableName, DeleteParameter deleteParameter, TransactionNo transactionNo) throws DuplicateDeleteException {
        ITable table = this.tableManager.getTableData(tableName);
        int retCount = 0;

        List<Object[]> allData = new ArrayList<Object[]>(table.getRecodeSize());

        // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
        TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, deleteParameter, table);

        NormalWhereParameter normalWhereParameter = deleteParameter.nextNormalWhereParameter();
        NormalWhereExecutor normalWhereExecutor = null;
        if (normalWhereParameter != null) {
            normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
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
        allData = null;
        return retCount;
    }

}