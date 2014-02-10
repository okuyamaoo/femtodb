package femtodb.core.table.data;import java.util.*;import java.util.concurrent.locks.Lock;import java.util.concurrent.locks.ReadWriteLock;import java.util.concurrent.locks.ReentrantReadWriteLock;import femtodb.core.table.*;import femtodb.core.table.transaction.*;public class TableDataTransfer {    private TransactionNo tn = null;    private LocalData localData = null;    //private Map<Long, ColumnData> storeDataMap = null;    private boolean storeFlg = false;    private boolean deletedFlg = false;    public TableDataTransfer() {        this.localData = new LocalData();    }    public TableDataTransfer clone4Update() {        TableDataTransfer tableDataTransfer = new TableDataTransfer();        tableDataTransfer.putLocalData(localData.getInnerData());        return tableDataTransfer;    }    public TableDataTransfer clone4Delete() {        TableDataTransfer tableDataTransfer = new TableDataTransfer();        return tableDataTransfer;    }    public String[] getInnerDataStringList() {        return this.localData.getInnerDataStringList();    }    public void putInnerDataStringList(String[] stringList) {        this.localData.putInnerDataStringList(stringList);    }    private void putLocalData(Object[] innerData) {        this.localData = new LocalData();        this.localData.putInnerData(innerData);    }    public void setTransactionNo(TransactionNo tn) {        this.tn = tn;    }    public TransactionNo getTransactionNo() {        return this.tn;    }    public boolean isDeletedData() {        return deletedFlg;    }/*    public void storeData(long oid, Map<Long, ColumnData> storeDataMap) {        this.oid = oid;        this.storeDataMap = storeDataMap;        this.storeDataMap.put(oid, new ColumnData(localData));        this.storeFlg = true;        this.localData = null;    }*/    public String getColumnData(String columnName) {        if (storeFlg) {            //ColumnData columnData = this.storeDataMap.get(this.oid);            //return columnData.getData(columnName);            return null;        } else {                        return this.localData.get(columnName);        }    }    public void setColumnData(String columnName, String data) {        if (storeFlg) {//            ColumnData columnData = this.storeDataMap.get(this.oid);//            columnData.setData(columnName, data);        } else {            this.localData.put(columnName, data);        }    }    public void delete() {        this.deletedFlg = true;    }    public void saveTmpData() {        // TODO:なんだかの方法でlocaldataを効率化する    }    public String toString() {        String ret = "";        ret = ret + "localData:" + localData + ",";        ret = ret + "storeFlg:" + storeFlg + ",";        ret = ret + "deletedFlg:" + deletedFlg;        return ret;    }}