package femtodb.core.util;

import femtodb.core.*;

/** 
 * SystemLogクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class SystemLog {

    public final static void println(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG) System.out.println(obj);
    }

    public final static void queryLog(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG == true || FemtoDBConstants.DB_REQUEST_LOG) System.out.println(obj);
    }

}