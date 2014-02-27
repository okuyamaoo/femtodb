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
public class FemtoDBConnectorTable  extends HttpServlet { 

    /** 
     * テーブルを情報を全て取得する.<br>
     * パラメータなし<br>
     * 返却値はJSONフォーマット<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Map resultMap = new LinkedHashMap();
        List<TableInfo> tableList = FemtoHttpServer.dataAccessor.getTableList();
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");

        for (int idx = 0; idx < tableList.size(); idx++) {
            Map table = new LinkedHashMap();
            TableInfo tableInfo = tableList.get(idx);

            table.put("table-name", tableInfo.tableName);

            if (tableInfo.infomationMap.size() > 0) {

                List indexList = new ArrayList();
                Iterator ite = tableInfo.infomationMap.entrySet().iterator();
                while(ite.hasNext()) {
                    Map indexMap = new LinkedHashMap();

                    Map.Entry<String, IColumnType> targetEntry = (Map.Entry)ite.next();
                    String key = targetEntry.getKey();
                    IColumnType value = targetEntry.getValue();
                    
                    indexMap.put("column-name", key);
                    indexMap.put("colum-type", value.getTypeString());
                    indexList.add(indexMap);
                }

                table.put("index", indexList);
            }
            resultMap.put(tableInfo.tableName, table);
        }
        String jsonStr = JSON.encode(resultMap);
        response.setContentLength(jsonStr.getBytes().length);
        response.getWriter().println(jsonStr);
    }


    /** 
     * テーブルを新規で作成する.<br>
     * 本リクエストはパラメータとして以下を必要とする.<br>
     * "table" : テーブル名を指定する<br>
     * 以下は必須ではないが適時指定する<br>
     * "indexcoolumns" : 検索インデックスを作成するカラム名を指定する。複数指定する際は","区切りで指定する<br>
     * インデックスの種類には完全一致の条件時に利用されるインデックス"equal"とテキスト検索時に利用される"text"が存在する<br>
     * "equal"はデータ取得時の条件" = "で利用される。<br>
     * "text"はデータ取得時の条件" text "で利用される。<br>
     * これらのインデックス種類を指定する際はカラ名の後ろに":equal"もしくは":text"と指定する<br>
     * 指定を省略した際は":equal"となる<br>
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
        SystemLog.println(request.getParameterMap());

        String tableName = request.getParameter("table");
        if (tableName == null || tableName.trim().equals("")) {

            response.setStatus(400);
            response.setContentType("text/html; charset=utf-8");
            response.getWriter().println("errormessage:\"Parameter 'table' not found\"");
            return;
        }

        int result = -1;
        try {
            String indexColumns = request.getParameter("indexcolumns");
            String[] indexColumnList = null;
            if (indexColumns != null && !indexColumns.trim().equals("")) {
                indexColumnList = indexColumns.split(",");
            }
            TableInfo tableInfo = new TableInfo(tableName.trim().toLowerCase());
    
            if (indexColumnList != null) {
                for (int i = 0; i < indexColumnList.length; i++) {
                    if (indexColumnList[i].indexOf(":equal") != -1) {
                        tableInfo.addTableColumnInfo(indexColumnList[i].trim().toLowerCase().replaceAll(":equal",""), new ColumnTypeVarchar());
                    } else if (indexColumnList[i].indexOf(":text") != -1) {
                        tableInfo.addTableColumnInfo(indexColumnList[i].trim().toLowerCase().replaceAll(":text",""), new ColumnTypeText());
                    } else if (indexColumnList[i].indexOf(":") == -1) {
                        tableInfo.addTableColumnInfo(indexColumnList[i].trim().toLowerCase(), new ColumnTypeVarchar());
                    } else {

                        response.setStatus(400);
                        response.setContentType("text/html; charset=utf-8");
                        response.getWriter().println("errormessage:\"Parameter 'indexcolumns' Format violation\"");
                        return;
                    }
                }
            }
            result = FemtoHttpServer.dataAccessor.createTable(tableInfo);
        } catch (Exception ee) {
            response.setStatus(500);
            response.setContentType("text/html; charset=utf-8");
            response.getWriter().println("errormessage:\"An unknown error has occurred\"");
            return;

        }


        if (result == 1) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().println("{\"result\":\"true\"}");
        } else if (result == 2) {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/html; charset=utf-8");
            response.getWriter().println("\"Table already exists\"");
        }
    }

    /** 
     * テーブルを削除.<br>
     * パラメータ：テーブル名<br>
     * 返却値はJSONフォーマット<br>
     * 本リクエストはパラメータとして以下を必要とする.<br>
     * "table" : テーブル名を指定する<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Map resultMap = new LinkedHashMap();
        
        String tableName = request.getParameter("table");
        if (tableName == null || tableName.trim().equals("")) {

            response.setStatus(400);
            response.setContentType("text/html; charset=utf-8");
            response.getWriter().println("errormessage:\"Parameter 'table' not found\"");
            return;
        }

        TableInfo tableInfo = FemtoHttpServer.dataAccessor.removeTable(tableName);

        if (tableInfo == null) {
            resultMap.put("result", "false");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            String jsonStr = JSON.encode(resultMap);
            response.setContentLength(jsonStr.getBytes().length);
            response.getWriter().println(jsonStr);
        } else {
            resultMap.put("result", "true");
            resultMap.put("tablename", tableName);

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            String jsonStr = JSON.encode(resultMap);
            response.setContentLength(jsonStr.getBytes().length);
            response.getWriter().println(jsonStr);
        }
    }
}
