package femtodb.core.table;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class TableManager {

    public static AtomicLong globalOid = null;

    public Object tableCreateObj = new Object();

    public Map<String, TableInfo> tableInfoMap = null;
    public Map<String, ITable> tableDataMap = null;


    public static void initOid() {
        TableManager.globalOid = new AtomicLong();
    }

    public static void initOid(long setOid) {
        TableManager.globalOid = new AtomicLong(setOid);
    }

    public long nextOid() {
        return TableManager.globalOid.incrementAndGet();
    }

    public TableManager() {
        this.tableInfoMap = new ConcurrentHashMap<String, TableInfo>();
        this.tableDataMap = new ConcurrentHashMap<String, ITable>();
    }

    public boolean existTable(String tableName) {
        return this.tableInfoMap.containsKey(tableName);
    }

    public int create(TableInfo tableInfo) {
        this.tableInfoMap.put(tableInfo.tableName, tableInfo);
        this.tableDataMap.put(tableInfo.tableName, new DefaultTable(tableInfo));

        return 1;
    }

    public TableInfo getTableInfo(String tableName) {
        return this.tableInfoMap.get(tableName);
    }

    public ITable getTableData(String tableName) {
        return this.tableDataMap.get(tableName);
    }
}