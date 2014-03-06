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
import femtodb.core.accessor.*;

/** 
 * DefaultTableクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DefaultTable extends AbstractTable implements ITable {

    private TableInfo tableInfo = null;
    public String[] indexColumnNames = null;

    public DataMap dataMap = null;
    public Map<String, IndexMap> indexsMap = null;
    public Map<String, TableData> uniqueKeyMap = null;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock uniqueMapReadLock = lock.readLock();
    private Lock uniqueMapWriteLock = lock.writeLock();


    public DefaultTable(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        this.indexColumnNames = this.tableInfo.getIndexColumnNameList();
        this.dataMap = new DataMap();
        this.indexsMap = new ConcurrentHashMap<String, IndexMap>();
        this.uniqueKeyMap = new ConcurrentHashMap<String, TableData>(100000);

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

    }

    public String[] getIndexColumnNames() {
        return this.indexColumnNames;
    }

    public DataMap getDataMap() {
        return this.dataMap;
    }

    public Map<String, IndexMap> getIndexsMap() {
        return this.indexsMap;
    }

    public String getTableName() {
        return this.tableInfo.tableName;
    }

    public List<String> getColumnNameList() {
        List<String> retList = new ArrayList();
        for (String key : this.tableInfo.infomationMap.keySet()) {
            retList.add(key);
        }
        return retList;
    }


    public boolean addTableData(TableData data) {

        String uniqueKey = data.getUniqueKey();
        if (uniqueKey == null) {
            this.dataMap.put(data.oid, data);
        } else {
            uniqueMapWriteLock.lock();
            try {
                this.dataMap.put(data.oid, data);
                this.uniqueKeyMap.put(uniqueKey, data);
            } finally {
                uniqueMapWriteLock.unlock();
            }
        }
        return true;
    }

    /**
     * コミット前の一次データを削除する
     *
     */
    public boolean removeTmpData(long oid) {
        TableData tableData = this.dataMap.remove(oid);
        removeUniqueKeyData(tableData);
        return true;
    }

    private void removeUniqueKeyData(TableData tableData) {
        if (tableData !=null) {
            uniqueMapWriteLock.lock();
            try {
                TableData rmTargetTableData = this.uniqueKeyMap.get(tableData.getUniqueKey());
                if (rmTargetTableData != null) {
                    if (rmTargetTableData.oid == tableData.oid) {
                        this.uniqueKeyMap.remove(tableData.getUniqueKey());
                    }
                }
            } finally {
                uniqueMapWriteLock.unlock();
            }
        }
    }

    // 未使用
    public boolean modTableData(TableData data) {
        return true;
    }

    // 未使用
    public boolean deleteTableData(TableData data) {
        return true;
    }

    public TableDataTransfer getTableData4UniqueKey(TransactionNo tn, String uniqueKey) {
        TableData tableData = uniqueKeyMap.get(uniqueKey);
        if (tableData != null) {
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(tn);
            return tableDataTransfer;
        }
        return null;
    }

    public TableIterator getTableDataIterator() {

        TableIterator tableIterator = new DefaultTableIterator(this.dataMap.getIterator());
        return tableIterator;
    }

    public TableIterator getTableDataIterator(TransactionNo tn, SelectParameter selectParameter) {
        // ここでIndexを適応
        if (selectParameter.existIndexWhereParameter()) {
            // Indexの指定があるため、indexsMapより該当カラムのインデックスマップより取得する
            NormalWhereParameter indexWhereParameter = selectParameter.getIndexWhereParameter();
            String indexColumnName = indexWhereParameter.getColumnName();
            IWhereParameter indexParameter = indexWhereParameter.getParameter();

            IndexMap indexMap = this.indexsMap.get(indexColumnName);
            if (indexMap == null) {
                TableIterator tableIterator = new DefaultTableIterator(this.dataMap.getIterator());
                return tableIterator;
            } else {
                TableIterator tableIterator = new IndexTableIterator(getTableName(), tn, indexMap, indexColumnName, indexParameter.toString());
                return tableIterator;
            }
        } else {
            TableIterator tableIterator = new DefaultTableIterator(this.dataMap.getIterator());
            return tableIterator;
        }
    }


    public int getRecodeSize() {
        return this.dataMap.size();
    }

    public boolean rebuildIndex() {
        Iterator indexsIterator = indexsMap.entrySet().iterator();
        while(indexsIterator.hasNext()) {
            Map.Entry targetEntry = (Map.Entry)indexsIterator.next();
            IndexMap map = (IndexMap)targetEntry.getValue();
            map.rebuildIndex();
        }
        
        QueryOptimizer.lastRebuildIndexInfo.put(tableInfo.tableName, System.nanoTime());
        return true;
    }



    public boolean cleanDeletedData() {
        DataMapIterator dataMapIterator = dataMap.getIterator();
        int cnt = 0;
        while(dataMapIterator.hasNext()) {
            cnt++;
            if ((cnt % 100) == 0) Thread.yield();
            dataMapIterator.next();
            TableData tableData = dataMapIterator.getValue();

            if (tableData != null) {

                TableDataTransfer oldData = tableData.oldData;
                TableDataTransfer newData = tableData.newData;

                boolean deleted = false;
                if (newData != null && newData.getTransactionNo().isCommited() == true && newData.isDeletedData()) {
                    dataMap.remove(dataMapIterator.getKey());
                    deleted = true;
                } else {
                    if (newData != null && newData.getTransactionNo().isCommited() == false) {
                        if (oldData.isDeletedData()) {
                            dataMap.remove(dataMapIterator.getKey());
                            deleted = true;
                        }
                    }
                }

                if (deleted) {
                    String uniqueKey = tableData.getUniqueKey();
                    if (uniqueKey != null) {
                        removeUniqueKeyData(tableData);
                    }
                }
            } else {
                dataMap.remove(dataMapIterator.getKey());
            }
        }
        return true;
    }

}