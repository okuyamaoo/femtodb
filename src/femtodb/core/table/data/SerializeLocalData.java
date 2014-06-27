package femtodb.core.table.data;import java.io.*;import java.util.*;/**  * SerializeLocalDataクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class SerializeLocalData extends AbstractLocalData implements LocalData, Serializable {    byte[][] data = new byte[5][];    static byte[] lfBytes = "\n".getBytes();    static byte[] tabBytes = "\t".getBytes();    public SerializeLocalData() {    }        public Object[] getInnerData() {        Object[] ret = new Object[5];        ret[0] = data[0];        ret[1] = data[1];        ret[2] = data[2];        ret[3] = data[3];        ret[4] = data[4];        return ret;    }    public void putInnerData(Object[] dataList) {        data[0] = (byte[])dataList[0];        data[1] = (byte[])dataList[1];        data[2] = (byte[])dataList[2];        data[3] = (byte[])dataList[3];        data[4] = (byte[])dataList[4];    }    public String[] getInnerDataStringList() {        String[] ret = new String[5];        if (data[0] != null) ret[0] = new String(data[0]);        if (data[1] != null) ret[1] = new String(data[1]);        if (data[2] != null) ret[2] = new String(data[2]);        if (data[3] != null) ret[3] = new String(data[3]);        if (data[4] != null) ret[4] = new String(data[4]);        return ret;    }    public void putInnerDataStringList(String[] stringList) {        if (stringList[0] != null) data[0] = stringList[0].getBytes();        if (stringList[1] != null) data[1] = stringList[1].getBytes();        if (stringList[2] != null) data[2] = stringList[2].getBytes();        if (stringList[3] != null) data[3] = stringList[3].getBytes();        if (stringList[4] != null) data[4] = stringList[4].getBytes();    }    public final void put(String key, String value) {        String keyFirstChar = key.substring(0, 1);        String groupStr = getColumnNameGroup(keyFirstChar, this);        if (groupStr == null) {            groupStr = key + "\t" + value;        } else {            String chkKey = key+"\t";            String[] groupStrList = groupStr.split("\n");            StringBuilder strBuf = new StringBuilder(100);            String sep ="";            boolean update = false;            for (int i = 0; i < groupStrList.length; i++) {                if (groupStrList[i].indexOf(chkKey) == 0) {                    groupStrList[i] = chkKey + value;                    update = true;                }                strBuf.append(sep);                strBuf.append(groupStrList[i]);                sep = "\n";            }            if (update == false) strBuf.append("\n").append(chkKey+value);            groupStr = strBuf.toString();        }                putColumnNameGroup(keyFirstChar, groupStr, this);    }/*    public final byte[] getBytes(String key) {        String ret = get(key);        if (ret == null) return null;        return ret.getBytes();    }*/    public final byte[] getBytes(String key) {        String ret = null;        String keyFirstChar = key.substring(0, 1);        byte[] groupStrBytes = getColumnNameGroupBytes(keyFirstChar, this);        if (groupStrBytes == null) return null;        byte[] chkKeyBytes = key.getBytes();        if (bytesIndexOf(groupStrBytes, lfBytes) == -1) {            if (bytesIndexOf(groupStrBytes, chkKeyBytes, tabBytes) == 0) {                byte[] retBytes = new byte[(groupStrBytes.length - (chkKeyBytes.length + 1))];                System.arraycopy(groupStrBytes, (chkKeyBytes.length+1), retBytes, 0, retBytes.length);                return retBytes;            }        }        byte[][] groupStrList = splitBytes(groupStrBytes, lfBytes);        for (int i = 0; i < groupStrList.length; i++) {            if (groupStrList[i] != null) {                if (bytesIndexOf(groupStrList[i], chkKeyBytes, tabBytes) == 0) {                    byte[] retBytes = new byte[(groupStrList[i].length - (chkKeyBytes.length + 1))];                    System.arraycopy(groupStrList[i], (chkKeyBytes.length+1), retBytes, 0, retBytes.length);                    return retBytes;                }            }        }        return null;    }    private byte[][] splitBytes(byte[] targetData, byte[] splitPt) {        byte[][] ret = null;        int size = 0;        for (int i = 0; i < targetData.length; i++) {            if (targetData[i] == splitPt[0]) size++;        }        ret = new byte[size][];        int start = 0;        int cnt = 0;        for (int i = 0; i < targetData.length; i++) {            if (targetData[i] == splitPt[0]) {                byte[] data = new byte[i];                System.arraycopy(targetData, start, data, 0, data.length);                ret[cnt] = data;                cnt++;                start = i+1;            }        }        return ret;    }    private int bytesIndexOf(byte[] targetBytes, byte[] searchBytes) {        int ret = -1;        if (searchBytes.length == 1) {            for (int i = 0; i < targetBytes.length; i++) {                if (targetBytes[i] == searchBytes[0]) {                    ret = i;                    break;                }            }        }        return ret;    }    private int bytesIndexOf(byte[] targetBytes, byte[] searchBytes, byte[] secondSearchBytes) {        int ret = 0;        if (targetBytes[0] != searchBytes[0]) return -1;        if (targetBytes.length < (searchBytes.length + 1)) return -2;                for (int i = 0; i < searchBytes.length; i++) {            ret++;            if (searchBytes[i] != targetBytes[i]) {                return -3;            }        }        if (targetBytes[ret] != secondSearchBytes[0]) return -4;        return 0;    }    public final String get(String key) {        String ret = null;        String keyFirstChar = key.substring(0, 1);        String groupStr = getColumnNameGroup(keyFirstChar, this);        if (groupStr == null) return null;        String chkKey = key+"\t";        if (groupStr.indexOf("\n") == -1) {            if (groupStr.indexOf(chkKey) == 0) {                String retStr = groupStr.substring(chkKey.length());                return retStr;            }        }        String[] groupStrList = groupStr.split("\n");        for (int i = 0; i < groupStrList.length; i++) {            if (groupStrList[i].indexOf(chkKey) == 0) {                ret = groupStrList[i].substring(chkKey.length());                return ret;            }        }        return ret;    }    public Map<String, String> getAllData() {        Map<String, String> allData = new TreeMap();        // 全てのデータをKeyとValueのセットで返す        Object[] byteList = new Object[5];        byteList[0] = data[0];        byteList[1] = data[1];        byteList[2] = data[2];        byteList[3] = data[3];        byteList[4] = data[4];        for (int i = 0; i < byteList.length; i++) {            if (byteList[i] != null) {                String dataStr = new String((byte[])byteList[i]);                String[] dataList = dataStr.split("\n");                for (int t = 0; t < dataList.length; t++) {                    if (!dataList[t].trim().equals("")) {                        String[] keyValue = dataList[t].split("\t");                        if (keyValue.length > 1) {                            allData.put(keyValue[0], keyValue[1]);                        } else {                            allData.put(keyValue[0], "");                        }                    }                }            }        }        return allData;    }}