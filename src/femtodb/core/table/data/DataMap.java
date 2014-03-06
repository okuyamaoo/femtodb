package femtodb.core.table.data;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import femtodb.core.util.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.index.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;

/** 
 * DataMap<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DataMap extends BaseMap {
    

    public DataMap() {
        super();
    }

    public void put(Long key, TableData value) {
        super.put(key, value);
    }

    public TableData get(Long key) {
        return (TableData)super.get(key);
    }

    public TableData remove(Long key) {
        return (TableData)super.remove(key);
    }

    public DataMapIterator getIterator() {
        return new DataMapIterator(this);
    }

    public int size() {
        return super.size();
    }
}