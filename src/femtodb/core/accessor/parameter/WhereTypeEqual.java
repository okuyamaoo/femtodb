package femtodb.core.accessor.parameter;import femtodb.core.table.data.*;public class WhereTypeEqual implements IWhereType {    private String whereParamStr = null;    private String columnName = null;    public WhereTypeEqual(String columnName, IWhereParameter whereParameter) {        this.columnName = columnName;        whereParamStr = whereParameter.toString();    }    public int type() {        return 1;    }    public boolean execute(TableDataTransfer tableDataTransfer) {        String columnData = tableDataTransfer.getColumnData(this.columnName);        if (columnData == null) return false;        return columnData.equals(whereParamStr);    }}