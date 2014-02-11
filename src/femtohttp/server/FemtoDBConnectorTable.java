package femtohttp.server;



import java.io.IOException;
 
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

public class FemtoDBConnectorTable  extends HttpServlet { 

    /** 
     * テーブルを新規で作成する.<br>
     * 本リクエストはパラメータとして以下を必要とする.<br>
     * "table" : テーブル名を指定する<br>
     * 以下は必須ではないが適時指定する<br>
     * "indexcoolumns" : 検索インデックスを作成するカラム名を指定する。複数指定する際は","区切りで指定する<br>
     *<br>
     * 返却値はJSONフォーマットで{"result":"true","errormessage":"メッセージ"}となり"result"は処理の正否を表す。左辺の文字列は"true" or "false"である<br>
     * "errormessage"はエラー発生時にメッセージが含まれる<br>
     * <br>
     *URL例)<br>
     * /femtodb/table?table=usermaster&indexcolumns=id,name<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String tableName = request.getParameter("table");
        if (tableName == null || tableName.trim().equals("")) {
            StringBuilder strBuf = new StringBuilder();
            strBuf.append("{\"result\":\"");
            strBuf.append("false");
            strBuf.append("\",\"errormessage\":\"Parameter 'table' not found\"}");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().println(strBuf.toString());
            return;
        }

        int result = -1;
        try {
            String indexColumns = request.getParameter("indexcoolumns");
            String[] indexColumnList = null;
            if (indexColumns != null && indexColumns.trim().equals("")) {
                indexColumnList = indexColumns.split(",");
            }
            TableInfo tableInfo = new TableInfo(tableName.trim().toLowerCase());
    
            if (indexColumnList != null) {
                for (int i = 0; i < indexColumnList.length; i++) {
                    tableInfo.addTableColumnInfo(indexColumnList[i].trim().toLowerCase(), new ColumnTypeVarchar());
                }
            }
            result = FemtoHttpServer.dataAccessor.createTable(tableInfo);
        } catch (Exception ee) {
            StringBuilder strBuf = new StringBuilder();
            strBuf.append("{\"result\":\"");
            strBuf.append("false");
            strBuf.append("\",\"errormessage\":\"An unknown error has occurred\"}");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().println(strBuf.toString());
            return;

        }


        StringBuilder strBuf = new StringBuilder();
        if (result == 1) {
            strBuf.append("{\"result\":\"true\"}");
        } else if (result == 2) {
            strBuf.append("{\"result\":\"false\",\"errormessage\":\"Table already exists\" }");

        }
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(strBuf.toString());
    }
}
