package femtodb.core.util;

import femtodb.core.*;

public class SystemLog {

    public final static void println(Object obj){
        if (FemtoDBConstants.DB_SYSTEM_LOG) System.out.println(obj);
    }

}