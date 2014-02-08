package femtodb.core.accessor;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;



public class SelectTableAccessor {


    TableManager tableManager = null;

    public SelectTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public List<TableDataTransfer> select(SelectParameter selectParameter, TransactionNo transactionNo) {
        if (selectParameter.existNormalWhereParameter() || selectParameter.existIndexWhereParameter()) {
            
            return select(selectParameter.getTableName(), selectParameter, transactionNo);
        } else {
            // 全件取得
            return select(selectParameter.getTableName(), transactionNo);
        }
    }

    /**
     * 全件取得
     *
     *
     */
    protected List<TableDataTransfer> select(String tableName, TransactionNo transactionNo) {
        ITable table = this.tableManager.getTableData(tableName);
        List<TableDataTransfer> allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());

        TableIterator iterator = table.getTableDataIterator();
        for (; iterator.hasNext();) {

            iterator.nextEntry();
            TableData tableData = (TableData)iterator.getEntryValue();
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
            if (tableDataTransfer != null) {
                allData.add(tableDataTransfer);
            }
        }
        return allData;
    }


    /**
     * 条件適応
     *
     *
     */
    protected List<TableDataTransfer> select(String tableName, SelectParameter selectParameter, TransactionNo transactionNo) {
        ITable table = this.tableManager.getTableData(tableName);
        List<TableDataTransfer> allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());

        // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
        TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, selectParameter, table);

        NormalWhereParameter normalWhereParameter = selectParameter.nextNormalWhereParameter();
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
                        allData.add(tableDataTransfer);
                    }
                } else {
                    allData.add(tableDataTransfer);
                }
            }
        }

        while ((normalWhereParameter = selectParameter.nextNormalWhereParameter()) != null) {
            normalWhereExecutor = null;
            normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
            if (allData.size() > 0) {
                int size = allData.size();
                List<TableDataTransfer> tmpAllData = new ArrayList<TableDataTransfer>(size);

                for (int i = 0; i < size; i++) {
                    TableDataTransfer targetTableDataTransfer = allData.get(i);
                    if (normalWhereExecutor.execute(targetTableDataTransfer)) {
                        tmpAllData.add(targetTableDataTransfer);
                    }
                }
                allData = tmpAllData;
            }
        }
        return allData;
    }

}