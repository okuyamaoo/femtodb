package femtodb.core.accessor;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;



public class UpdateTableAccessor {


    TableManager tableManager = null;

    public UpdateTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public int update(UpdateParameter updateParameter, TransactionNo transactionNo) throws DuplicateUpdateException, Exception {

        if (updateParameter.existNormalWhereParameter() || updateParameter.existIndexWhereParameter()) {
            
            return update(updateParameter.getTableName(), updateParameter, transactionNo);
        } else {
            // 全件更新
            return updateAll(updateParameter.getTableName(), updateParameter, transactionNo);
        }
    }

    /**
     * 全件更新
     *
     *
     */
    protected int updateAll(String tableName, UpdateParameter updateParameter, TransactionNo transactionNo) throws DuplicateUpdateException {
        ITable table = this.tableManager.getTableData(tableName);
        int retCount = 0;

        TableIterator iterator = table.getTableDataIterator();
        List<UpdateColumnParameter> updateColumnList = updateParameter.getUpdateParameterList();
        int updateColumnListSize = updateColumnList.size();

        if (updateColumnList != null) {

            for (; iterator.hasNext();) {
    
                iterator.nextEntry();
                TableData tableData = (TableData)iterator.getEntryValue();
                TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
                TableDataTransfer upateTableDataTransfer = tableDataTransfer.clone4Update();

                for (int idx = 0; idx < updateColumnListSize; idx++) {
                    UpdateColumnParameter columnParam = updateColumnList.get(idx);
                    upateTableDataTransfer.setColumnData(columnParam.getColumnName(), columnParam.getColumnData());
                }
                try {
                    tableData.modData(transactionNo, upateTableDataTransfer);
                    retCount++;
                } catch (DuplicateUpdateException duplicateUpdateException) {
                    throw duplicateUpdateException;
                }
            }
        }
        return retCount;
    }


    /**
     * 条件適応
     *
     *
     */
    protected int update(String tableName, UpdateParameter updateParameter, TransactionNo transactionNo) throws DuplicateUpdateException {
        ITable table = this.tableManager.getTableData(tableName);
        int retCount = 0;

        List<UpdateColumnParameter> updateColumnList = updateParameter.getUpdateParameterList();
        int updateColumnListSize = updateColumnList.size();

        List<Object[]> allData = new ArrayList<Object[]>(table.getRecodeSize());

        // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
        TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, updateParameter, table);

        NormalWhereParameter normalWhereParameter = updateParameter.nextNormalWhereParameter();
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

        while ((normalWhereParameter = updateParameter.nextNormalWhereParameter()) != null) {
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

        int updateTargetDataSize = allData.size();
        if (updateColumnList != null) {

            for (int i = 0; i < updateTargetDataSize; i++) {

                Object[] data = allData.get(i);
                TableData tableData = (TableData)data[0];
                TableDataTransfer tableDataTransfer = (TableDataTransfer)data[1];
                TableDataTransfer upateTableDataTransfer = tableDataTransfer.clone4Update();

                for (int idx = 0; idx < updateColumnListSize; idx++) {
                    UpdateColumnParameter columnParam = updateColumnList.get(idx);
                    upateTableDataTransfer.setColumnData(columnParam.getColumnName(), columnParam.getColumnData());
                }
                try {
                    tableData.modData(transactionNo, upateTableDataTransfer);
                    retCount++;
                } catch (DuplicateUpdateException duplicateUpdateException) {
                    throw duplicateUpdateException;
                }

            }
        }
        allData = null;
        return retCount;
    }

}