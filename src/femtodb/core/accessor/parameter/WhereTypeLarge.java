package femtodb.core.accessor.parameter;import femtodb.core.table.data.*;public class WhereTypeLarge implements IWhereType {    private double whereParamDouble = 0.0;    private String columnName = null;    public WhereTypeLarge(String columnName, IWhereParameter whereParameter) {        this.columnName = columnName;        whereParamDouble = new Double(whereParameter.toString()).doubleValue();    }    public int type() {        return 3;    }    public boolean execute(TableDataTransfer tableDataTransfer) {        String columnData = tableDataTransfer.getColumnData(this.columnName);        if (columnData == null) return false;        try {            double columnDataDouble = new Double(columnData).doubleValue();            if (columnDataDouble > whereParamDouble) return true;            return false;        } catch (Exception e) {            return false;        }    }}