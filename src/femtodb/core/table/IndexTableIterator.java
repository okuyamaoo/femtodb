package femtodb.core.table;import java.util.*;import femtodb.core.table.*;import femtodb.core.table.data.*;import femtodb.core.table.index.*;import femtodb.core.table.transaction.*;/**  * IndexTableIteratorクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class IndexTableIterator implements TableIterator {    private String tableName = null;    private IndexMap indexMap = null;    private String targetColumnData = null;    private Map<Long, IndexStruct> indexGroupData = null;    private int nowCursor = 0;    private Map<Long, TableData> modDataMap = null;    private Iterator iterator = null;    private Map.Entry nowEntry = null;    public IndexTableIterator(String tableName, TransactionNo tn, IndexMap indexMap, String indexColumnName, String targetColumnData) {        this.tableName = tableName;        this.indexMap = indexMap;        this.targetColumnData = targetColumnData;        this.indexGroupData = this.indexMap.getIndexGroup(this.targetColumnData);        if (indexGroupData == null) this.indexGroupData = new HashMap();        this.modDataMap = tn.modTableFolder.get(tableName);        if (this.modDataMap != null) {            Iterator modIterator = this.modDataMap.entrySet().iterator();            while(modIterator.hasNext()) {                Map.Entry entry = (Map.Entry)modIterator.next();                Long key = (Long)entry.getKey();                TableData value = (TableData)entry.getValue();                TableDataTransfer targetTransfer = value.getTableDataTransfer(tn);                // nullの場合は同一トランザクション内で削除している                if (targetTransfer != null) {                                    String colData = targetTransfer.getColumnData(indexColumnName);                                        if (colData != null && colData.equals(targetColumnData)) {                        this.indexGroupData.put(key, new IndexStruct(tn, value));                    }                }            }                    }        this.iterator = this.indexGroupData.entrySet().iterator();    }    public boolean hasNext() {        return this.iterator.hasNext();    }    public void next() {        this.nowEntry = (Map.Entry)this.iterator.next();    }    public Object getEntryKey() {        return this.nowEntry.getKey();    }    public Object getEntryValue() {        IndexStruct indexStruct = (IndexStruct)this.nowEntry.getValue();                return indexStruct.getTableData();    }}