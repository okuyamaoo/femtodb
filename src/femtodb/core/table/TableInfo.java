package femtodb.core.table;

import java.io.*;
import java.util.*;

import femtodb.core.table.type.*;
import femtodb.core.*;

/** 
 * TableInfoクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TableInfo implements Serializable {

    public String tableName = null;
    public Map<String, IColumnType> infomationMap = new LinkedHashMap<String, IColumnType>();


    public TableInfo() {
    }

    public TableInfo(String tableName) {
        this.tableName = tableName;
    }

    public void addTableColumnInfo(String columnName, IColumnType columnType) throws TableInfoException {
        if (this.infomationMap.containsKey(columnName)) throw new TableInfoException(columnName + " column exist");
        
        this.infomationMap.put(columnName, columnType);
    }

    public boolean existColumn(String columnName, IColumnType columnType) {
        if (!this.infomationMap.containsKey(columnName)) return false;
        IColumnType targetColumnType = this.infomationMap.get(columnName);
        if (targetColumnType.getType() == columnType.getType()) return true;
        return false;
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

    public String createStoreString() {
        StringBuilder strBuf = new StringBuilder(100);
        strBuf.append(tableName);
        strBuf.append("\t");

        for (String key : this.infomationMap.keySet()) {
            strBuf.append(key);
            strBuf.append("\t");
            strBuf.append(this.infomationMap.get(key));
            strBuf.append("\t");
        }

        return strBuf.toString();
    }

    public void setupStoreString(String storeString) {
        String[] logString = storeString.split("\t");

        this.tableName = logString[0];
        try {
            if (logString.length > 1) {
                for (int i = 1; i < logString.length; i=i+2) {

                    if (logString[i+1].equals("Number type")) {
                        infomationMap.put(logString[i], new ColumnTypeNumber());
                    } else if (logString[i+1].equals("Varchar type")) {
                        infomationMap.put(logString[i], new ColumnTypeVarchar());
                    } else if (logString[i+1].equals("Text type")) {
                        infomationMap.put(logString[i], new ColumnTypeText());
                    }
                }
            }
        } catch (Exception e) {}
    }

    public String toString() {
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("TableName:" + tableName);
        strBuf.append("\n");
        strBuf.append("IndexColumn:"+infomationMap);
        return strBuf.toString();
    }
}