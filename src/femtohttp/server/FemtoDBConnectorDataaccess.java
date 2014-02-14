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

public class FemtoDBConnectorDataaccess extends HttpServlet { 

    /** 
     * データを取得する.<br>
     * 本メソッドは指定されたテーブルを指定された条件(条件は存在しない場合もある)のもとデータを検索し返却する.<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 検索を行うテーブル名を指定する<br>
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     * "where" : 検索条　複数指定可能、複数指定し配列とする<br>
     *  例)where:column1=abc<br>
     *    where:column2=201401<br>
     * "limit" : 取得件数<br>
     * "offset" : 取得開始位置(1始まり。指定した位置のデータを含む)<br>
     *  例)データが10件ある場合、offset:2と指定した場合2件目から全件取得となる<br>
     * "orderby" : ソートカラムと昇順、降順の指定。カラム名と指定を"+"で連結<br>
     *             並び順は辞書順である.<br>
     *             カラムの値が数値もしくはnullもしくは空白であることが保証出来る場合は指定の後方に"+number"と付けることで数値ソートが可能である.<br>
     *  例1)"orderby":column3+asc<br>
     *  例2)"orderby":column4+asc+number<br>
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        // 必須条件を取得
        try {
            String tableName = request.getParameter("table");
            TableInfo tableInfo = null;
            if (tableName == null || tableName.trim().equals("") || (tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName.trim().toLowerCase())) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'table' Parameter not found.");
                return;
            }
            tableName = tableName.trim().toLowerCase();
    
            // 自由パラメータ取得
            // TransactionNo
            // 妥当性チェックと返却
            String tnNoStr = request.getParameter("transactionno");
            long tansactionNo = -1L;
            if (tnNoStr != null) {
                // 指定あり
                // 妥当性確認
                if (!executeTransactionNoValidate(response, tnNoStr)) return;
                tansactionNo = new Long(tnNoStr).longValue();
            }
    
            // where取得
            String[] whereList = request.getParameterValues("where");
            if (whereList != null) {
                // 指定あり
                // 妥当性確認
                if (!executeWhereValidate(response, whereList)) return;
            }
    
            // TODO:limit, offset, orderby 未実装
    
    
            // トランザクションNoを指定していない場合は一回利用のTransactionNoオブジェクトを作成し番号を取得
            boolean onceTransaction = false;
            if (tansactionNo == -1) {
                tansactionNo = getTransactionNo();
                onceTransaction = true;
            }
    
            // クエリー実行
            // Select用のパラメータクラス作成
            SelectParameter sp = new SelectParameter();
            // Table指定
            sp.setTableName(tableInfo.tableName);
    
            // WhereはIndexと通常カラムで異なる
            settingWhereParameter(tableInfo, sp, whereList);
    
            // TODO : 以降limit, offset, orderby 未実装
            
            // 実行
            long queryStartTime = System.nanoTime();
            List resultList = FemtoHttpServer.dataAccessor.selectTableData(sp, tansactionNo);
            long queryEndTime = System.nanoTime();

            // TODO:JSONデコードする前にTableDataTransferからカラムとデータだけ抜き出してそれをMapにしてJSONデコードしないといけない
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().print(JSON.encode(resultList));
            response.getWriter().flush();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        }
    }

/*    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        this.executeRequest(request, response);
    }
*/
    /**
     * femtoserver?
     *
     *
     *
     */

    private long getTransactionNo() {
        TransactionNo tn = FemtoHttpServer.dataAccessor.createTransaction();
        return tn.getTransactionNo();
    }

    private void settingWhereParameter(TableInfo tableInfo, SelectParameter sp, String[] whereList) {
        if (whereList != null) {
            for (int idx = 0; idx < whereList.length; idx++) {
                String[] whereDetail = whereList[idx].split("=");
    
                String columneName = whereDetail[0].trim().toLowerCase();
                // 定義済みIndexカラムか通常のカラムの判断
                if (tableInfo.existColumn(columneName)) {
                    // 定義済みIndex
                    sp.setIndexWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));
                } else {
                    sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));
                }
            }
        }
    }

    private boolean executeTransactionNoValidate(HttpServletResponse response, String transactionNoStr) throws ServletException, IOException {
        try {
            long tansactionNo = -1L;
            if (transactionNoStr != null && !transactionNoStr.trim().equals("")) {
                // TransactionNoが指定されている可能性
                // 数値チェック
                try {
                    Long tnNoChk = new Long(transactionNoStr);
                    tansactionNo = tnNoChk.longValue();
                    
                    if (tansactionNo < 0) {
                        response.setContentType("text/html");
                        response.setStatus(400);
                        response.getWriter().println("'transactionno' Can specify only positive number.");
                        return false;
                    }
                } catch (NumberFormatException nfe) {
                    // テーブル名なし
                    response.setContentType("text/html");
                    response.setStatus(400);
                    response.getWriter().println("'transactionno' Can specify only numbers.");
                    return false;
                }
            }
        } catch (Exception e) {
            throw e;
        }
        return true;
    }

    private boolean executeWhereValidate(HttpServletResponse response, String[] whereList) throws ServletException, IOException {
        try {

            // カラム名=値　のフォーマットである必要がある
            for (int idx = 0; idx < whereList.length; idx++) {
                String whereDt = whereList[idx];
                if (whereDt == null || whereDt.trim().equals("")) {
                    response.setContentType("text/html");
                    response.setStatus(400);
                    response.getWriter().println("'where' Format violation.");
                    return false;
                } else {
                    String[] checkWork = whereDt.split("=");
                    if (checkWork.length != 2) {
                        response.setContentType("text/html");
                        response.setStatus(400);
                        response.getWriter().println("'where' Format violation.");
                        return false;
                    }
                }
            }
        } catch (Exception e) {
            throw e;
        }
        return true;
    }




}
