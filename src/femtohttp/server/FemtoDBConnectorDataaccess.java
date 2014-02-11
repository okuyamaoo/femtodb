package femtohttp.server;


import java.io.IOException;
 
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class FemtoDBConnectorDataaccess  extends HttpServlet { 

    /** 
     * データを取得する.<br>
     * 本メソッドは指定されたテーブルを指定された条件(条件は存在しない場合もある)のもとデータを検索し返却する.<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 検索を行うテーブル名を指定する<br>
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     * "table" : テーブル名<br>
     * "table" : テーブル名<br>
     *
     *
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        this.executeRequest(request, response);
/*        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>FemtoServer</h1>");*/
    }

    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        this.executeRequest(request, response);
    }

    /**
     * femtoserver?
     *
     *
     *
     */
    private void executeRequest(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            
        } catch (Exception e) {
        }
    }
}
