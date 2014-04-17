package femtohttp.server;


import java.util.*;
import java.io.IOException;

import net.arnx.jsonic.JSON;
 
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import femtodb.core.*;
import femtodb.core.util.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.data.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;

/** 
 * FemtoDBConnectorTableクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class FemtoDBConnectorUtil  extends HttpServlet { 

    /** 
     * FemtoDBのサーバ上にバックアップを作成する.<br>
     * パラメータなし<br>
     * 返却値はJSONフォーマット<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        SystemLog.queryLog(request.getParameterMap());
        Map resultMap = new LinkedHashMap();
        long start = System.nanoTime();
        boolean createResult = FemtoHttpServer.dataAccessor.storeTableObject();
        long end = System.nanoTime();
        resultMap.put("Backup result", createResult);
        resultMap.put("Backup create time", ((end - start) / 1000 /1000) + " ms");
       
        String jsonStr = JSON.encode(resultMap);
        response.setContentLength(jsonStr.getBytes().length);
        response.getWriter().println(jsonStr);
    }

}
