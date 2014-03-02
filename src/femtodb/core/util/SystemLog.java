package femtodb.core.util;

import java.util.*;
import java.util.Calendar;
import java.text.SimpleDateFormat;

import femtodb.core.*;

/** 
 * SystemLogクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class SystemLog {

    public static SimpleDateFormat sysoutFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    public static Calendar cal = Calendar.getInstance();

    public final static void println(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG) {
            System.out.println(sysoutFormat.format(cal.getTime()) + ":Debug log:" + obj);
        }
    }

    public final static void queryLog(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG == true || FemtoDBConstants.DB_REQUEST_LOG) System.out.println(sysoutFormat.format(cal.getTime()) + ":Query log:" + obj);
    }


    public final static void queryPriorityLog(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG == true || FemtoDBConstants.DB_REQUEST_EXECUTION_PRIORITY_LOG) System.out.println(sysoutFormat.format(cal.getTime()) + ":Query priority log:" + obj);
    }

}