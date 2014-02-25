package femtodb.core.table;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import femtodb.core.table.data.*;
import femtodb.core.util.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.table.index.*;
import femtodb.core.table.type.*;


public class DefaultTable extends AbstractTable implements ITable {

    private TableInfo tableInfo = null;
    public String[] indexColumnNames = null;

    public Map<Long, TableData> dataMap = null;
    public Map <String, IndexMap> indexsMap = null;


    //public Map<String, ColumnDataList> columnDataMap = null; // カラム名とそのカラムのデータのリスト


    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();


    public DefaultTable(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.indexColumnNames = this.tableInfo.getIndexColumnNameList();
        this.dataMap = new ConcurrentHashMap<Long, TableData>(1000000);
        this.indexsMap = new ConcurrentHashMap<String, IndexMap>();


        if (indexColumnNames != null && indexColumnNames.length > 0) {
            for (int idx = 0; idx < indexColumnNames.length; idx++) {

                if (this.tableInfo.getColumnType(indexColumnNames[idx]).getType() == IColumnType.VARCHAR_COLUMN) { 
                    // FullMatch用Index
                    this.indexsMap.put(indexColumnNames[idx], new FullMatchIndexMap(indexColumnNames[idx], new CharacterIndexComparator(), dataMap));
                } else if (this.tableInfo.getColumnType(indexColumnNames[idx]).getType() == IColumnType.NUMBER_COLUMN) {
                    // NumberRange用Index
                    this.indexsMap.put(indexColumnNames[idx], new NumberRangeIndexMap(indexColumnNames[idx], new NumberIndexComparator(), dataMap));
                } else if (this.tableInfo.getColumnType(indexColumnNames[idx]).getType() == IColumnType.TEXT_COLUMN) {
                    // TextSearch用Index
                    this.indexsMap.put(indexColumnNames[idx], new TextMatchIndexMap(indexColumnNames[idx], new CharacterIndexComparator(), dataMap));
                }
            }
        }

        //this.columnDataMap = new ConcurrentHashMap<String, ColumnDataList>(8192);
    }

    public String[] getIndexColumnNames() {
        return this.indexColumnNames;
    }

    public Map<Long, TableData> getDataMap() {
        return this.dataMap;
    }

    public Map <String, IndexMap> getIndexsMap() {
        return this.indexsMap;
    }

    public String getTableName() {
        return this.tableInfo.tableName;
    }

    public List<String> getColumnNameList() {
        readLock.lock();
        try {
            List<String> retList = new ArrayList();
            for (String key : this.tableInfo.infomationMap.keySet()) {
                retList.add(key);
            }
            return retList;
        } finally {
            readLock.unlock();
        }
    }


    public boolean addTableData(TableData data) {

        this.dataMap.put(data.oid, data);
        return true;
    }

    /**
     * コミット前の一次データを削除する
     *
     */
    public boolean removeTmpData(long oid) {
//        SystemLog.println(oid);
//        SystemLogprintln("remove befor size=" + this.dataMap.size());
        this.dataMap.remove(oid);
//        SystemLog.println("remove after size=" + this.dataMap.size());
        return true;
    }

    public boolean modTableData(TableData data) {
        // TODO:Indexデータも変更
        return true;
    }

    public boolean deleteTableData(TableData data) {
        // TODO:Indexデータも変更
        return true;
    }

    public TableIterator getTableDataIterator() {

        TableIterator tableIterator = new DefaultTableIterator(this.dataMap.entrySet().iterator());
        return tableIterator;
    }

    public TableIterator getTableDataIterator(TransactionNo tn, SelectParameter selectParameter) {
        // TODO:ここでIndexを適応したい
        if (selectParameter.existIndexWhereParameter()) {
            // Indexの指定があるため、indexsMapより該当カラムのインデックスマップより取得する
            NormalWhereParameter indexWhereParameter = selectParameter.getIndexWhereParameter();
            String indexColumnName = indexWhereParameter.getColumnName();
            IWhereParameter indexParameter = indexWhereParameter.getParameter();

            IndexMap indexMap = this.indexsMap.get(indexColumnName);
            if (indexMap == null) {
                TableIterator tableIterator = new DefaultTableIterator(this.dataMap.entrySet().iterator());
                return tableIterator;
            } else {
                TableIterator tableIterator = new IndexTableIterator(getTableName(), tn, indexMap, indexColumnName, indexParameter.toString());
                return tableIterator;
            }
        } else {
            TableIterator tableIterator = new DefaultTableIterator(this.dataMap.entrySet().iterator());
            return tableIterator;
        }
    }


    public int getRecodeSize() {
        readLock.lock();
        try {
            return this.dataMap.size();
        } finally {
            readLock.unlock();
        }
    }

    public boolean rebuildIndex() {
        Iterator indexsIterator = indexsMap.entrySet().iterator();
        while(indexsIterator.hasNext()) {
            Map.Entry targetEntry = (Map.Entry)indexsIterator.next();
            IndexMap map = (IndexMap)targetEntry.getValue();
            map.rebuildIndex();
        }
        return true;
    }
}