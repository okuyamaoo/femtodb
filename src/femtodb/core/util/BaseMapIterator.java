package femtodb.core.util;

import java.util.*;

/** 
 * BaseMapIterator<br>
 * BaseMap用のIterator<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class BaseMapIterator {

    private BaseMap baseMap = null;
    private int startSize = 0;
    private int cursorPoint = 0;
    private Object[] nextData = null;
    private boolean nextGet = true;
    private List baseMapDataList = null;

    public BaseMapIterator(BaseMap baseMap) {
    
        startSize = baseMap.lastSize;
        this.baseMap = baseMap;
        baseMapDataList = baseMap.dataList;
        startSize = baseMapDataList.size();
    }

    public boolean hasNext() {
        if (this.baseMap.lastSize <= cursorPoint) return false;
        nextData = (Object[])baseMapDataList.get(cursorPoint);
        if (nextData[0] != null) {
            nextGet = false;
            return true;
        } else {
            cursorPoint++;
        }

        while (this.baseMap.lastSize > cursorPoint) {

            nextData = (Object[])baseMapDataList.get(cursorPoint);
            if (nextData[0] != null) {
                nextGet = false;
                return true;
            } else {
                cursorPoint++;
            }
        }
        return false;
    }

    public void next() {
        if (nextGet == false) {
            cursorPoint++;
            nextGet = true;
        }
    }

    public Object getKey() {
        if (nextGet == false) {
            cursorPoint++;
            nextGet = true;
        }
        return nextData[0];
    }

    public Object getValue() {
        if (nextGet == false) {
            cursorPoint++;
            nextGet = true;
        }
        return nextData[1];
    }
}

