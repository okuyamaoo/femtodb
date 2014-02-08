package femtodb.core.table;

import java.util.*;


import femtodb.core.table.type.*;
import femtodb.core.*;

public class TableInfo {

    public String tableName = null;
    public Map<String, IColumnType> infomationMap = new LinkedHashMap<String, IColumnType>();

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }


    public void addTableColumnInfo(String columnName, IColumnType columnType) throws TableInfoException {
        if (this.infomationMap.containsKey(columnName)) throw new TableInfoException(columnName + " column exist");
        this.infomationMap.put(columnName, columnType);
    }

    public boolean existColumn(String columnName) {
        return this.infomationMap.containsKey(columnName);
    }

    public IColumnType getColumnType(String columnName) {
        return this.infomationMap.get(columnName);
    }

    public String[] getIndexColumnNameList() {
        String[] keyList = new String[infomationMap.size()];
        int idx = 0;
        for (String key : this.infomationMap.keySet()) {
            keyList[idx] = key;
            idx++;
        }
        return keyList;
    }

    public String toString() {
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("TableName:" + tableName);
        strBuf.append("\n");
        strBuf.append("Column:"+infomationMap);
        return strBuf.toString();
    }
}