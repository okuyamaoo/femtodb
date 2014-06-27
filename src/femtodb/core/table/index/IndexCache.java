package femtodb.core.table.index;import java.io.*;import java.util.*;import java.util.concurrent.locks.Lock;import java.util.concurrent.locks.ReadWriteLock;import java.util.concurrent.locks.ReentrantReadWriteLock;import java.security.MessageDigest;import java.security.NoSuchAlgorithmException;public class IndexCache extends LinkedHashMap {    private int capacity = 1000;    private ReadWriteLock lock = new ReentrantReadWriteLock(true);    private Lock readLock = lock.readLock();    private Lock writeLock = lock.writeLock();    private String tmpPath = "./tmp";    private static Object[] sync = new Object[25];    static {        for (int i = 0; i < 25; i++) {            sync[i] = new Integer(1);        }    }    public IndexCache(int capacity) {        super(capacity, 0.75f, true);        this.capacity = capacity;    }    public Object put(Object key, Object value) {    System.out.println("put");        writeLock.lock();        try {            if (super.containsKey(key)) {                super.put(key, value);            } else {                String digestVal = digest(key.toString());                File chkTmpFile = new File(tmpPath + "/" + digestVal);                if (chkTmpFile.exists()) {                    // ファイル有り                    super.put(key, value);                    chkTmpFile.delete();                } else {                    // ファイルなし                    super.put(key, value);                }            }        } catch (Exception e) {            e.printStackTrace();        } finally {            writeLock.unlock();        }        return null;    }    public Object remove(Object key) {    System.out.println("remove");        writeLock.lock();        try {            Object retObj = null;            if (super.containsKey(key)) {                retObj = super.remove(key);            }            String digestVal = digest(key.toString());            File chkTmpFile = new File(tmpPath + "/" + digestVal);            if (chkTmpFile.exists()) {                // ファイル有り                chkTmpFile.delete();            }            return retObj;        } catch (Exception e) {            e.printStackTrace();        } finally {            writeLock.unlock();        }        return null;    }    public Object get(Object key) {    System.out.println("get");        readLock.lock();        try {            Object retVal = super.get(key);            if (retVal != null) {                return retVal;            } else {                String digestVal = digest(key.toString());                File chkTmpFile = new File(tmpPath + "/" + digestVal);                if (chkTmpFile.exists()) {                    // ファイル有り                                        Object value = readObject(chkTmpFile);                    if (value == null) {                        value = super.get(key);                    } else {                        super.put(key, value);                    }                    return value;                }            }        } catch (Exception e) {            e.printStackTrace();        } finally {            readLock.unlock();        }        return null;    }    public boolean containsKey(Object key) {    System.out.println("cont");        readLock.lock();        try {            Object retVal = super.get(key);            if (retVal != null) {                return true;            } else {                String digestVal = digest(key.toString());                File chkTmpFile = new File(tmpPath + "/" + digestVal);                if (chkTmpFile.exists()) {                    // ファイル有り                    Object value = readObject(chkTmpFile);                    if (value == null) value = super.get(key);                    if (value != null) return true;                    return false;                }            }        } catch (Exception e) {            e.printStackTrace();        } finally {            readLock.unlock();        }        return false;    }    private String digest(String str) {        MessageDigest md = null;        StringBuffer strBuf = new StringBuffer();        try {            md = MessageDigest.getInstance("SHA-256");            md.update(str.getBytes());            byte[] digest = md.digest();             for (int i = 0; i < digest.length; i++) {                strBuf.append(String.format("%02x", digest[i]));            }         } catch (Exception e) {            e.printStackTrace();        }        return strBuf.toString();    }        private Object readObject(File chkTmpFile) {        FileInputStream fis = null;        ObjectInputStream ois = null;        try {            fis = new FileInputStream(chkTmpFile);            ois = new ObjectInputStream(fis);            Object retObj = ois.readObject();                        return retObj;        } catch (Exception e) {            e.printStackTrace();        } finally {            try {                if (ois != null) {                    ois.close();                }                if (fis != null) {                    fis.close();                }            } catch (Exception ee) {            }        }        return null;    }    private boolean writeObject(String fileName, Object target) {        FileOutputStream fos = null;        ObjectOutputStream oos = null;        try {            fos = new FileOutputStream(fileName);            oos = new ObjectOutputStream(fos);            oos.writeObject(target);            return true;        } catch (Exception e) {            e.printStackTrace();        } finally {            try {                if (oos != null) {                    oos.close();                }                if (fos != null) {                    fos.close();                }            } catch (Exception ee) {            }        }        return false;    }    protected boolean removeEldestEntry(Map.Entry eldest) {    System.out.println(size());        if (size() > this.capacity) {            Object key = eldest.getKey();            synchronized (sync[((key.hashCode() << 1) >>> 1) % 25]) {                if (writeObject(tmpPath + "/" + digest(key.toString()), eldest.getValue())) {                    return true;                }            }            return false;        }        return  false;    }}