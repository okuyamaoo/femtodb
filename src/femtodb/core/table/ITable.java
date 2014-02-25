package femtodb.core.table;

import java.util.*;

import femtodb.core.table.data.*;
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

    public Map<Long, TableData> getDataMap();

    public Map <String, IndexMap> getIndexsMap();

    public String getTableName();

    public List<String> getColumnNameList();

    public boolean addTableData(TableData data);

    public boolean removeTmpData(long oid);

    public boolean modTableData(TableData data);
    
    public boolean deleteTableData(TableData data);
    
    public TableIterator getTableDataIterator();

    public TableIterator getTableDataIterator(TransactionNo tn, SelectParameter selectParameter);

    public boolean rebuildIndex();

    public int getRecodeSize();

}