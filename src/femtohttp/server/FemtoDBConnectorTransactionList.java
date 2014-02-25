package femtohttp.server;


import java.util.*;
import java.io.IOException;
 
import net.arnx.jsonic.JSON;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.data.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;



public class FemtoDBConnectorTransactionList  extends HttpServlet { 

    /** 
     * 現在有効なトランザクションの一覧を返す.<br>
     * commit or rollbackされたTransactionは返らない<br>
     * 返却値としてTransaction番号が返される<br>
     * 本リクエストはパラメータを必要としない<br>
     * 返却値はJSONフォーマット<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        
        List<TransactionNo> list = FemtoHttpServer.dataAccessor.getTransactionNoList();
        List<Map> retList = new ArrayList();
        for (TransactionNo tn : list) {
            Map tnInfo = new LinkedHashMap();
            tnInfo.put("No", tn.getTransactionNo());
            tnInfo.put("Create date", tn.getDateString());
            retList.add(tnInfo);
        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(JSON.encode(retList));
    }
}
