package femtodb.core.table.data;import java.io.*;import java.util.*;/**  * SerializeLocalDataクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class SerializeLocalData extends AbstractLocalData implements LocalData, Serializable {    byte[] data1 = null;    byte[] data2 = null;    byte[] data3 = null;    byte[] data4 = null;    byte[] data5 = null;    public SerializeLocalData() {    }        public Object[] getInnerData() {        Object[] ret = new Object[5];        ret[0] = data1;        ret[1] = data2;        ret[2] = data3;        ret[3] = data4;        ret[4] = data5;        return ret;    }    public void putInnerData(Object[] dataList) {        data1 = (byte[])dataList[0];        data2 = (byte[])dataList[1];        data3 = (byte[])dataList[2];        data4 = (byte[])dataList[3];        data5 = (byte[])dataList[4];    }    public String[] getInnerDataStringList() {        String[] ret = new String[5];        if (data1 != null) ret[0] = new String(data1);        if (data2 != null) ret[1] = new String(data2);        if (data3 != null) ret[2] = new String(data3);        if (data4 != null) ret[3] = new String(data4);        if (data5 != null) ret[4] = new String(data5);        return ret;    }    public void putInnerDataStringList(String[] stringList) {        if (stringList[0] != null) data1 = stringList[0].getBytes();        if (stringList[1] != null) data2 = stringList[1].getBytes();        if (stringList[2] != null) data3 = stringList[2].getBytes();        if (stringList[3] != null) data4 = stringList[3].getBytes();        if (stringList[4] != null) data5 = stringList[4].getBytes();    }    public final void put(String key, String value) {        String keyFirstChar = key.substring(0, 1);        String groupStr = getColumnNameGroup(keyFirstChar, this);        if (groupStr == null) {            groupStr = key + "\t" + value;        } else {            String chkKey = key+"\t";            String[] groupStrList = groupStr.split("\n");            StringBuilder strBuf = new StringBuilder(100);            String sep ="";            boolean update = false;            for (int i = 0; i < groupStrList.length; i++) {                if (groupStrList[i].indexOf(chkKey) == 0) {                    groupStrList[i] = chkKey + value;                    update = true;                }                strBuf.append(sep);                strBuf.append(groupStrList[i]);                sep = "\n";            }            if (update == false) strBuf.append("\n").append(chkKey+value);            groupStr = strBuf.toString();        }                putColumnNameGroup(keyFirstChar, groupStr, this);    }    public final byte[] getBytes(String key) {        String ret = get(key);        if (ret == null) return null;        return ret.getBytes();    }    public final String get(String key) {        String ret = null;        String keyFirstChar = key.substring(0, 1);        String groupStr = getColumnNameGroup(keyFirstChar, this);                if (groupStr == null) return null;                String chkKey = key+"\t";        if (groupStr.indexOf("\n") == -1) {            if (groupStr.indexOf(chkKey) == 0) {                return groupStr.substring(chkKey.length());            }        }        String[] groupStrList = groupStr.split("\n");        for (int i = 0; i < groupStrList.length; i++) {            if (groupStrList[i].indexOf(chkKey) == 0) {                ret = groupStrList[i].substring(chkKey.length());                return ret;            }        }        return ret;    }    public Map<String, String> getAllData() {        Map<String, String> allData = new TreeMap();        // 全てのデータをKeyとValueのセットで返す        Object[] byteList = new Object[5];        byteList[0] = data1;        byteList[1] = data2;        byteList[2] = data3;        byteList[3] = data4;        byteList[4] = data5;        for (int i = 0; i < byteList.length; i++) {            if (byteList[i] != null) {                String dataStr = new String((byte[])byteList[i]);                String[] dataList = dataStr.split("\n");                for (int t = 0; t < dataList.length; t++) {                    if (!dataList[t].trim().equals("")) {                        String[] keyValue = dataList[t].split("\t");                        if (keyValue.length > 1) {                            allData.put(keyValue[0], keyValue[1]);                        } else {                            allData.put(keyValue[0], "");                        }                    }                }            }        }        return allData;    }}