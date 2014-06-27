package femtodb.core.table;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.type.*;
import femtodb.core.table.data.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.index.*;


/** 
 * ITable<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public interface ITable {

    public String[] getIndexColumnNames();

    public DataMap getDataMap();

    public Map <String, IndexMap> getIndexsMap();

    public String getTableName();

    public List<String> getColumnNameList();

    public TableDataTransfer getTableData4UniqueKey(TransactionNo transactionNo , String uniqueKey);

    public boolean addTableData(TableData data);

    public boolean removeTmpData(long oid);

    public TableData getTableData(long oid);

    public boolean modTableData(TableData data);
    
    public boolean deleteTableData(TableData data);
    
    public TableIterator getTableDataIterator();

    public TableIterator getTableDataIterator(TransactionNo tn, SelectParameter selectParameter);

    public boolean addIndexColumn(String columnName, IColumnType indexType);

    public boolean addIndexColumnInfo(String columnName, IColumnType indexType) throws TableInfoException;

    public boolean rebuildIndex(QueryOptimizer queryOptimizer);

    public boolean createAllDataIndex(TransactionNo transactionNo);

    public boolean cleanDeletedData();

    public int getRecodeSize();

}