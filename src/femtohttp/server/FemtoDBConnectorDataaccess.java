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

    private boolean debug = true;

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
        if (debug) System.out.println(request.getParameterMap());
        boolean onceTransaction = false;
        long tansactionNo = -1L;

        // 必須条件を取得
        try {
            String tableName = request.getParameter("table");
            TableInfo tableInfo = null;
            if (tableName == null || tableName.trim().equals("")) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'table' Parameter not found.");
                return;
            }

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName.trim().toLowerCase())) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }
            tableName = tableName.trim().toLowerCase();
    
            // 自由パラメータ取得
            // TransactionNo
            // 妥当性チェックと返却
            String tnNoStr = request.getParameter("transactionno");
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
    
            // limit offset取得
            String limitStr = request.getParameter("limit");
            if (limitStr != null) {
                // 指定あり
                // 妥当性確認
                if (!executeLimitValidate(response, limitStr)) return;
            }

            String offsetStr = request.getParameter("offset");
            if (offsetStr != null) {
                // 指定あり
                // 妥当性確認
                if (!executeOffsetValidate(response, offsetStr)) return;
            }

            // TODO:orderby 未実装
            
    
            // トランザクションNoを指定していない場合は一回利用のTransactionNoオブジェクトを作成し番号を取得
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

            // limit
            settingLimitParameter(sp, limitStr);

            // offset
            settingOffsetParameter(sp, offsetStr);

            // TODO : orderby 未実装
            
            // 実行
            long queryStartTime = System.nanoTime();
            List<TableDataTransfer> resultList = FemtoHttpServer.dataAccessor.selectTableData(sp, tansactionNo);
            long queryEndTime = System.nanoTime();

            // JSONデコードする前にTableDataTransferからカラムとデータだけ抜き出してそれをMapにしてJSONデコード
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().print("[");
            // 返却用のデータへ変換
            String sep = "";
            for (int idx = 0; idx < resultList.size(); idx++) {
                response.getWriter().print(sep);
                TableDataTransfer tableDataTransfer = resultList.get(idx);
                response.getWriter().print(JSON.encode(tableDataTransfer.getDataMap()));
                if (idx > 0 && ((idx % 50) == 0)) response.getWriter().flush();
                if (idx == 0) sep = ",";
            }
            response.getWriter().print("]");
            response.getWriter().flush();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        } finally {
            if (onceTransaction) {
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
            }
        }
    }

    /** 
     * データを登録する.<br>
     * 本メソッドは指定されたテーブル指定されたデータを登録する.<br>
     * 登録するデータはKey=Valueのデータの複数の集合を1つのデータの集まりとして指定したテーブルへ保存する.<br>
     * 登録するデータは複数個指定可能である。
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 登録を行うテーブル名を指定する<br>
     * "data" : JSONフォーマットでのKey=Value景色の集合(連想配列)
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (debug) System.out.println(request.getParameterMap());
        boolean onceTransaction = false;
        long tansactionNo = -1L;

        // 必須条件を取得
        try {
            String tableName = request.getParameter("table");
            TableInfo tableInfo = null;
            if (tableName == null || tableName.trim().equals("")) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'table' Parameter not found.");
                return;
            }

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName.trim().toLowerCase())) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }
            tableName = tableName.trim().toLowerCase();
    
            // TransactionNo
            // 妥当性チェックと返却
            String tnNoStr = request.getParameter("transactionno");

            if (tnNoStr != null) {
                // 指定あり
                // 妥当性確認
                if (!executeTransactionNoValidate(response, tnNoStr)) return;
                tansactionNo = new Long(tnNoStr).longValue();
            }

            // 登録データ部分取得
            String[] dataList = request.getParameterValues("data");
            if (dataList == null || dataList.length < 1) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'data' Parameter not found.");
                return;
            }

            // 全てのデータの妥当性を検証し全てエラーでない場合のみ登録
            // 検証と同時にMapとして取得
            List<Map<String, String>> insertDataList = new ArrayList();
            if (!executeDataValidateAndConvert(response, dataList, insertDataList)) return;

            // 登録処理
            // TransactionNoが指定されていない場合作成
            if (tansactionNo == -1) {
                tansactionNo = getTransactionNo();
                onceTransaction = true;
            }

            try {
                for (int idx = 0; idx < insertDataList.size(); idx++) {
                    TableDataTransfer tableDataTransfer = new TableDataTransfer();
    
                    Map<String, String> insertData = insertDataList.get(idx);
                    for (Iterator ite = insertData.entrySet().iterator(); ite.hasNext();) {
                        Map.Entry<String, String> entry = (Map.Entry)ite.next();
                        String key = entry.getKey();
                        String value = entry.getValue();
                        tableDataTransfer.setColumnData(key, value);
                    }
                    FemtoHttpServer.dataAccessor.insertTableData(tableName, tansactionNo, tableDataTransfer);
                }
            } catch (ClassCastException cce) {
                // 登録データがString=Stringの形式でない
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'data' Format violation.");
                return;
            }

            // TransactionNo指定なしの場合ここでCommit
            if (onceTransaction) {
                FemtoHttpServer.dataAccessor.commitTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
                tansactionNo = -1L;
            }

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().print("{\"result\":" +insertDataList.size() + "}");
            response.getWriter().flush();
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        } finally {
            if (onceTransaction) {
                if (tansactionNo != -1L) {
                    FemtoHttpServer.dataAccessor.rollbackTransaction(tansactionNo);
                    FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
                }
            }
        }
    }

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
                    if (debug) System.out.println("where - index Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");
                    sp.setIndexWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));
                } else {
                    if (debug) System.out.println("where - normal Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");
                    sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));
                }
            }
        }
    }

    private void settingLimitParameter(SelectParameter sp, String limitStr) {
        if (limitStr != null) {
            sp.setLimit(new Integer(limitStr).intValue());
        }
    }
    
    private void settingOffsetParameter(SelectParameter sp, String offsetStr) {
        if (offsetStr != null) {
            sp.setOffset(new Integer(offsetStr).intValue());
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

    private boolean executeLimitValidate(HttpServletResponse response, String limitStr) throws ServletException, IOException {

        // limitは正数である必要がある
        try {
            new Integer(limitStr);
        } catch (NumberFormatException e) {
            response.setContentType("text/html");
            response.setStatus(400);
            response.getWriter().println("'limit' Format violation.");
            return false;
        }
        return true;
    }


    private boolean executeOffsetValidate(HttpServletResponse response, String offsetStr) throws ServletException, IOException {

        // offsetは正数である必要がある
        try {
            new Integer(offsetStr);
        } catch (NumberFormatException e) {
            response.setContentType("text/html");
            response.setStatus(400);
            response.getWriter().println("'offset' Format violation.");
            return false;
        }
        return true;
    }


    private boolean executeDataValidateAndConvert(HttpServletResponse response, String[] dataList, List convertStoreList) throws ServletException, IOException {

        // 登録データをJSON形式からMapへ変換
        try {

            for (int idx = 0; idx < dataList.length; idx++) {
                Map<String, String> targetData = (Map<String, String>)JSON.decode(dataList[idx]);

                convertStoreList.add(targetData);
            }
        } catch (Exception e) {
            response.setContentType("text/html");
            response.setStatus(400);
            response.getWriter().println("'data' Format violation.");
            return false;
        }
        return true;
    }
}
