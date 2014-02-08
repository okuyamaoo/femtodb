package femtodb.core.table.index;import femtodb.core.table.data.*;import femtodb.core.table.transaction.*;public class IndexStruct {    private long transactionNo = -1L;    private TableData tableData = null;    public IndexStruct(TransactionNo tn, TableData tableData) {        this.transactionNo = tn.getTransactionNo();        this.tableData = tableData;    }    public long getTransactionNo() {        return transactionNo;    }    public TableData getTableData() {        return tableData;    }    public String toString() {        return "IndexStruct: transactionNo=" + transactionNo + " tableData=" + tableData;    }}