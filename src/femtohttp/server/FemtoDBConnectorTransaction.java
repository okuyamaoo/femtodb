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

public class FemtoDBConnectorTransaction  extends HttpServlet { 

    /** 
     * トランザクションを新規で作成する.<br>
     * 返却値としてTransaction番号が返される.<br>
     * 本リクエストはパラメータを必要としない.<br>
     * 返却値はJSONフォーマットで{"transactionno":1}となり左辺の数値Long値であり可変となる<br>
     * ここで取得した数値を以降のデータアクセス系やcommit、rollback、トランザクション終了などのパラメータとして利用する<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        TransactionNo tn = FemtoHttpServer.dataAccessor.createTransaction();
        long transactionNo = tn.getTransactionNo();
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("{\"transactionno\":");
        strBuf.append(transactionNo);
        strBuf.append("}");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(strBuf.toString());
    }

    /** 
     * トランザクションをCommitもしくはRollbackする.<br>
     * 返却値として成否が返される.<br>
     * 本リクエストはパラメータとして以下を必要とする.<br>
     * "transactionno" 対象とするトランザクションの番号 <br>
     * "method" "commit"及び、"rollback"を指定<br>
     *<br><br>
     * 返却値はJSONフォーマットで{"result":"true"}となり左辺の文字列は"true" or "false"である<br>
     *<br>
     * URLの例 /femtodb/transaction?transactionno=23&method=commit<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String transactionNo = request.getParameter("transactionno");
        long transactioNoLong = -1L;
        try {
            transactioNoLong = new Long(transactionNo).longValue();
        } catch (Exception e2) {
            StringBuilder strBuf = new StringBuilder();
            strBuf.append("{\"result\":\"");
            strBuf.append("false");
            strBuf.append("\"}");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().println(strBuf.toString());
            return;
        }

        String method = request.getParameter("method");
        boolean result = false;

        if (method.equals("commit")) {
            // commit処理
            result = FemtoHttpServer.dataAccessor.commitTransaction(transactioNoLong);
        } else if (method.equals("rollback")) {
            // rollback処理
            result = FemtoHttpServer.dataAccessor.rollbackTransaction(transactioNoLong);
        }
        
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("{\"result\":\"");
        strBuf.append(result);
        strBuf.append("\"}");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(strBuf.toString());
    }


    /** 
     * トランザクションを終了する.<br>
     * 返却値として成否が返される.<br>
     * 本リクエストはパラメータとして以下を必要とする.<br>
     * "transactionno" 対象とするトランザクションの番号 <br>
     *<br><br>
     * 返却値はJSONフォーマットで{"result":"true"}となり左辺の文字列は"true" or "false"である<br>
     *<br>
     * URLの例 /femtodb/transaction?transactionno=23<br>
     *
     * @param request
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String transactionNo = request.getParameter("transactionno");
        long transactioNoLong = -1L;
        try {
            transactioNoLong = new Long(transactionNo).longValue();
        } catch (Exception e2) {
            StringBuilder strBuf = new StringBuilder();
            strBuf.append("{\"result\":\"");
            strBuf.append("false");
            strBuf.append("\"}");
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().println(strBuf.toString());
            return;
        }

        String method = request.getParameter("method");
        boolean result = false;

        if (method.equals("commit")) {
            // commit処理
            result = FemtoHttpServer.dataAccessor.endTransaction(transactioNoLong);
        } else if (method.equals("rollback")) {
            // rollback処理
            result = FemtoHttpServer.dataAccessor.endTransaction(transactioNoLong);
        }
        
        StringBuilder strBuf = new StringBuilder();
        strBuf.append("{\"result\":\"");
        strBuf.append(result);
        strBuf.append("\"}");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType("application/json; charset=utf-8");
        response.getWriter().println(strBuf.toString());
    }

}