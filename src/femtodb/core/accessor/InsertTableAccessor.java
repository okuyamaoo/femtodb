package femtodb.core.accessor;

import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;


/** 
 * InsertTableAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class InsertTableAccessor {


    public TableManager tableManager = null;

    public InsertTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public int insert(String tableName, TransactionNo transactionNo, TableDataTransfer tableDataTransfer) {
        ITable table = this.tableManager.getTableData(tableName);
        TableData tableData = new TableData(this.tableManager.nextOid(), 
                                            transactionNo, 
                                            this.tableManager.getTableInfo(tableName),
                                            table,
                                            tableDataTransfer);
        table.addTableData(tableData);
        return 1;
    }
}