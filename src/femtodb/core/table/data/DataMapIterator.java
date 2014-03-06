package femtodb.core.table.data;


import java.io.*;
import java.util.*;
import java.net.*;
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
 * DataMapIterator<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DataMapIterator extends BaseMapIterator {

    public DataMapIterator(DataMap dataMap) {
        super(dataMap);
    }

    public boolean hasNext() {
        return super.hasNext();
    }


    public void next() {
        super.next();
    }

    public Long getKey() {
        return (Long)super.getKey();
    }

    public TableData getValue() {
        return (TableData)super.getValue();
    }
}
