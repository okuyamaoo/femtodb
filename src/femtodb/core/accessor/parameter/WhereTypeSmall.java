package femtodb.core.accessor.parameter;import java.util.*;import femtodb.core.*;import femtodb.core.table.*;import femtodb.core.table.data.*;import femtodb.core.table.transaction.*;import femtodb.core.accessor.parameter.*;import femtodb.core.accessor.executor.*;import femtodb.core.accessor.scripts.*;/**  * WhereTypeSmallクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class WhereTypeSmall implements IWhereType {    private double whereParamDouble = 0.0;    private String columnName = null;    public WhereTypeSmall(String columnName, IWhereParameter whereParameter) {        this.columnName = columnName;        whereParamDouble = new Double(whereParameter.toString()).doubleValue();    }    public int type() {        return 4;    }    public final boolean execute(TableDataTransfer tableDataTransfer) {        String columnData = tableDataTransfer.getColumnData(this.columnName);        if (columnData == null) return false;        try {            double columnDataDouble = new Double(columnData).doubleValue();            if (columnDataDouble < whereParamDouble) return true;            return false;        } catch (Exception e) {            return false;        }    }    public final void executeAll(TableIterator iterator, TransactionNo transactionNo, List resultList) {        for (; iterator.hasNext();) {            iterator.nextEntry();            TableData tableData = (TableData)iterator.getEntryValue();            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);            if (tableDataTransfer != null) {                String columnData = tableDataTransfer.getColumnData(this.columnName);                if (columnData == null) continue;                try {                    double columnDataDouble = new Double(columnData).doubleValue();                    if (columnDataDouble < whereParamDouble) resultList.add(tableDataTransfer);                } catch (Exception e) {                    continue;                }            }        }    }}