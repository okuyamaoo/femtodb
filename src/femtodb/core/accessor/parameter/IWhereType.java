package femtodb.core.accessor.parameter;import femtodb.core.table.data.*;public interface IWhereType {    public static int WHERE_TYPE_EQUAL = 1; // =    public static int WHERE_TYPE_LIKE = 2;  // LIKE    public static int WHERE_TYPE_LARGE = 3; // >    public static int WHERE_TYPE_SMALL = 4; // <    public int type();    public boolean execute(TableDataTransfer tableDataTransfer);}