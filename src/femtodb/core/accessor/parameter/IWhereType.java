package femtodb.core.accessor.parameter;import java.util.*;import femtodb.core.*;import femtodb.core.table.*;import femtodb.core.table.data.*;import femtodb.core.table.transaction.*;import femtodb.core.accessor.parameter.*;import femtodb.core.accessor.executor.*;import femtodb.core.accessor.scripts.*;/**  * IWhereTypeクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public interface IWhereType {    public static int WHERE_TYPE_EQUAL = 1; // =    public static int WHERE_TYPE_LIKE = 2;  // LIKE    public static int WHERE_TYPE_LARGE = 3; // >    public static int WHERE_TYPE_SMALL = 4; // <    public int type();    public boolean execute(TableDataTransfer tableDataTransfer);    public void executeAll(TableIterator iterator, TransactionNo transactionNo, List resultList);}