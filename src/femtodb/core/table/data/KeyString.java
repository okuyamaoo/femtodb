package femtodb.core.table.data;

import java.io.*;

/** 
 * KeyString<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class KeyString implements Serializable, Comparable {
    
    byte[] keyBytes = null;
    int hashCode = -1;

    public KeyString(String key) {
        hashCode = key.hashCode();
        keyBytes = key.getBytes();
    }

    public int hashCode() {
        return hashCode;
    }

    public byte[] getBytes() {
        return keyBytes;
    }


    public boolean equals(Object obj) {
        if (obj instanceof KeyString) {
            byte[] target = ((KeyString)obj).getBytes();
            if (target.length == keyBytes.length) {
                if ((target[target.length - 1] == keyBytes[keyBytes.length - 1]) && (target[0] == keyBytes[0])) {
                    for (int i = 0; i < target.length; i++) {
                        if (target[i] != keyBytes[i]) {
                            return false;
                        }
                    }
                    return true;
                }
            }
        }
        return false;
    }

    public String toString() {
        return new String(keyBytes);
    }

    
    public int compareTo(Object keyString) {
        String o1 = new String(((KeyString)keyString).toString());
        String o2 = this.toString();
        return o2.compareTo(o1);
    }
}