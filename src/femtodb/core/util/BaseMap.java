package femtodb.core.util;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.table.index.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;

/** 
 * BaseMapクラス<br>
 * 並列アクセスが可能であり、Iteratorに類似したループアクセスがHashMapよりも高速である<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class BaseMap {
    
    private Map mainData = new HashMap(1000000);
    public List dataList = new ArrayList(1000000);
    private List removeList = new ArrayList(1000000);

    int lastSize = 0;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    public Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    

    public BaseMap() {
    }

    public void put(Object key, Object value) {
        writeLock.lock();
        try {
            Integer index = (Integer)mainData.get(key);
            if (index == null) {
                int newIndex = dataList.size();
                if (removeList.size() > 0) {

                    newIndex = ((Integer)removeList.remove(0)).intValue();
                    Object[] data = (Object[])dataList.get(newIndex);
                    data[0] = key;
                    data[1] = value;
                } else {
                    Object[] data = new Object[2];
                    data[0] = key;
                    data[1] = value;

                    dataList.add(data);
                }

                mainData.put(key, newIndex);
                lastSize = mainData.size();
            } else {
                Object[] data = (Object[])dataList.get(index.intValue());
                data[1] = value;
            }
        } finally {
            writeLock.unlock();
        }
    }


    public Object get(Object key) {
        readLock.lock();
        try {
            Integer index = (Integer)mainData.get(key);
            if (index == null) return null;
            Object[] ret = (Object[])dataList.get(index.intValue());
            return ret[1];
        } finally {
            readLock.unlock();
        }
    }

    public Object remove(Object key) {
        writeLock.lock();
        try {
            Integer index = (Integer)mainData.remove(key);
            if (index == null) return null;

            removeList.add(index);
            Object[] ret = (Object[])dataList.get(index.intValue());

            lastSize = mainData.size();

            Object retObj = ret[1];
            ret[0] = null;
            ret[1] = null;

            return retObj;
        } finally {
            writeLock.unlock();
        }
    }

    public BaseMapIterator getIterator() {
        return new BaseMapIterator(this);
    }

    public int size() {
        return lastSize;
    }
}
