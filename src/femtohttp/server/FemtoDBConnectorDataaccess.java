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


public class FemtoDBConnectorDataaccess extends AbstractFemtoDBServlet { 

    /** 
     * データを取得する.<br>
     * 本メソッドは指定されたテーブルを指定された条件(条件は存在しない場合もある)のもとデータを検索し返却する.<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 検索を行うテーブル名を指定する<br>
     * 必須ではないが指定可能なパラメータは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     * "where" : 検索条　複数指定可能、複数指定し配列とする<br>
     * 指定可能な条件は以下<br>
     * =:左辺のカラムのデータが右辺で指定したデータと完全一致<br>
     * text:左辺のカラムのデータ内に右辺で指定したデータが部分一致<br>
     * >:左辺のカラムのデータ内に右辺で指定した値よりも大きい値に一致<br>
     * <:左辺のカラムのデータ内に右辺で指定した値よりも小さい値に一致<br>
     *&nbsp;&nbsp;これらの条件はテーブル作成時のインデックス作成たカラム、していないカラムを含め全てに適応出来るが、<br>
     *&nbsp;&nbsp;インデックス作成したカラムの方が高速に検索可能である<br>
     *&nbsp;&nbsp;※インデックス作成可能な条件は"=" or "text"のみである<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;例)where:column1&nbsp;&nbsp;=&nbsp;&nbsp;abc<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where:column2&nbsp;&nbsp;text&nbsp;&nbsp;201401<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where:column3&nbsp;&nbsp;>&nbsp;&nbsp;100<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where:column4&nbsp;&nbsp;<&nbsp;&nbsp;999<br>
     * "limit" : 取得件数<br>
     * "offset" : 取得開始位置(1始まり。指定した位置のデータを含む)<br>
     *  例)データが10件ある場合、offset:2と指定した場合2件目から全件取得となる<br>
     * "sort" : ソートカラムと昇順、降順の指定。カラム名と指定を" "で連結<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;並び順は辞書順である.<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;カラムの値が数値もしくはnullもしくは空白であることが保証出来る場合は指定の後方に"+number"と付けることで数値ソートが可能である.<br>
     *  例1)sort=column3&nbsp;&nbsp;asc<br>
     *  例2)sort=column4&nbsp;&nbsp;asc&nbsp;&nbsp;number<br>
     *  例3)sort=column4&nbsp;&nbsp;asc&nbsp;&nbsp;number,col1&nbsp;&nbsp;asc<br>
     *&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;複数のカラムをsortに利用する場合は適応させたい順番にカンマで区切りで指定<br>
     *<br>
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (debug) SystemLog.println(request.getParameterMap());
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

            // テーブル名小文字化
            tableName = tableName.trim().toLowerCase();

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName)) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }

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

            // sort取得
            String orderbyStr = request.getParameter("sort");
            if (orderbyStr != null) {
                // 指定あり
                // 妥当性確認
                if (!executeOrderbyValidate(response, orderbyStr)) return;
            }

    
            // トランザクションNoを指定していない場合は一回利用のTransactionNoオブジェクトを作成し番号を取得
            if (tansactionNo == -1) {
                tansactionNo = getTransactionNo();
                onceTransaction = true;
            }
    
            // クエリ部分
            // Select用のパラメータクラス作成
            SelectParameter sp = new SelectParameter();
            // Table指定
            sp.setTableName(tableInfo.tableName);

            // Where
            settingWhereParameter(tableInfo, sp, whereList);

            // limit
            settingLimitParameter(sp, limitStr);

            // offset
            settingOffsetParameter(sp, offsetStr);

            // orderby
            settingOrderbyParameter(sp, orderbyStr);

            // クエリ実行
            long queryStartTime = System.nanoTime();
            ResultStruct resultStruct = FemtoHttpServer.dataAccessor.selectTableData(sp, tansactionNo);
            List<TableDataTransfer> resultList = resultStruct.getResultList();
            long queryEndTime = System.nanoTime();

            // JSONデコードする前にTableDataTransferからカラムとデータだけ抜き出してそれをMapにしてJSONデコード
            response.setStatus(HttpServletResponse.SC_OK);

            StringBuilder retStrBuf = new StringBuilder(8192);

            response.setContentType("application/json; charset=utf-8");
            retStrBuf.append("{\"count\":");
            retStrBuf.append(resultStruct.getBaseResultCount());
            retStrBuf.append(", \"result\":");
            retStrBuf.append("[");
            // 返却用のデータへ変換
            String sep = "";
            for (int idx = 0; idx < resultList.size(); idx++) {
                retStrBuf.append(sep);
                TableDataTransfer tableDataTransfer = resultList.get(idx);
                retStrBuf.append(JSON.encode(tableDataTransfer.getDataMap()));
//                if (idx > 0 && ((idx % 50) == 0)) response.getWriter().flush();
                if (idx == 0) sep = ",";
            }
            retStrBuf.append("]}");
            response.getWriter().print(retStrBuf.toString());
            response.getWriter().flush();
            retStrBuf = null;
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
     * 登録するデータはKey=Valueのデータの複数の集合を1つのデータの集まりとして指定したテーブルへ保存する<br>
     * 登録するデータは複数個指定可能である。<br>
     * 返却値はJSON形式で{"result":登録件数}となる<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 登録を行うテーブル名を指定する<br>
     * "data" : JSONフォーマットでのKey=Value景色の集合(連想配列)<br>
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (debug) SystemLog.println(request.getParameterMap());
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

            // テーブル名小文字化
            tableName = tableName.trim().toLowerCase();

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName)) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }
    
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
            response.getWriter().print("{\"result\":" + insertDataList.size() + "}");
            response.getWriter().flush();
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        } finally {
            if (onceTransaction && tansactionNo != -1L) {
                FemtoHttpServer.dataAccessor.rollbackTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
            }
        }
    }

    /** 
     * データを削除する.<br>
     * 本メソッドは指定されたテーブル指定されたデータを削除する.<br>
     * 削除対象となるデータは条件にて任意で指定可能である<br>
     * 条件を指定しない場合はテーブル内のデータが全て削除される<br>
     * 返却値はJSON形式で{"result":削除件数}となる<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table" : 削除を行うテーブル名を指定する<br>
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     * "where" : 検索条　複数指定可能、複数指定し配列とする<br>
     *  例)where:column1=abc<br>
     *    where:column2=201401<br>
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doDelete(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (debug) SystemLog.println(request.getParameterMap());
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

            // テーブル名小文字化
            tableName = tableName.trim().toLowerCase();

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName)) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }
    
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

            // トランザクションNoを指定していない場合は一回利用のTransactionNoオブジェクトを作成し番号を取得
            if (tansactionNo == -1) {
                tansactionNo = getTransactionNo();
                onceTransaction = true;
            }

            // クエリ部分
            // Delete用のパラメータクラス作成
            DeleteParameter dp = new DeleteParameter();
            // Table指定
            dp.setTableName(tableInfo.tableName);

            // Where
            settingWhereParameter(tableInfo, dp, whereList);

            // クエリ実行
            long queryStartTime = System.nanoTime();
            int resultCount = FemtoHttpServer.dataAccessor.deleteTableData(dp, tansactionNo);
            long queryEndTime = System.nanoTime();

            // TransactionNo指定なしの場合ここでCommit
            if (onceTransaction) {
                FemtoHttpServer.dataAccessor.commitTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
                tansactionNo = -1L;
            }

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().print("{\"result\":" + resultCount + "}");
            response.getWriter().flush();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        } finally {
            if (onceTransaction && tansactionNo != -1L) {
                FemtoHttpServer.dataAccessor.rollbackTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
            }
        }
    }


    /** 
     * データを更新する.<br>
     * 本メソッドは指定されたテーブル指定されたデータを更新する.<br>
     * 更新対象となるデータは条件にて任意で指定可能である<br>
     * 条件を指定しない場合はテーブル内のデータが全て更新される<br>
     * 返却値はJSON形式で{"result":更新件数}となる<br>
     *
     * @param request 必須となるURLパラメータは以下となる<br>
     * "table"&nbsp;&nbsp;:&nbsp;&nbsp;更新録を行うテーブル名を指定する<br>
     * "data"&nbsp;&nbsp;:&nbsp;&nbsp;更新情報。カラム名=更新値のMap型をJSON形式で指定した文字列
     *&nbsp;&nbsp;例){"col1":"Update-col1","col2":"Update-col2"}
     * 必須ではないが指定可能なURLは以下<br>
     * "transactionno" : 予め取得したトランザクション番号<br>
     * "where" : 検索条　複数指定可能、複数指定し配列とする<br>
     *&nbsp;&nbsp;指定方式は全てdoGetと同様となる
     *  例)where:column1&nbsp;&nbsp;=&nbsp;&nbsp;abc<br>
     *    where:column2&nbsp;&nbsp;index&nbsp;&nbsp;201401<br>
     *
     * @param response 
     * @throws ServletException
     * @throws IOException
     */
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        if (debug) SystemLog.println(request.getParameterMap());
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

            // テーブル名小文字化
            tableName = tableName.trim().toLowerCase();

            // テーブルの存在確認
            if ((tableInfo = FemtoHttpServer.dataAccessor.getTableInfo(tableName)) == null) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'" + tableName + "' table not found.");
                return;
            }

            // 更新用の値取得
            String updateDataJson = (String)request.getParameter("data");
            if (updateDataJson == null || updateDataJson.trim().equals("")) {
                // テーブル名なし
                response.setContentType("text/html");
                response.setStatus(400);
                response.getWriter().println("'data' Parameter not found.");
                return;
            }
            // 検証と同時にMapとして取得
            Map<String, String> updateData = new HashMap();
            if (!executeDataValidateAndConvert(response, updateDataJson, updateData)) return;


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

            // トランザクションNoを指定していない場合は一回利用のTransactionNoオブジェクトを作成し番号を取得
            if (tansactionNo == -1) {
                tansactionNo = getTransactionNo();
                onceTransaction = true;
            }

            // クエリ部分
            // Update用のパラメータクラス作成
            UpdateParameter up = new UpdateParameter();
            // Table指定
            up.setTableName(tableInfo.tableName);
            // Updateカラム指定
            Iterator ite = updateData.entrySet().iterator();
            while(ite.hasNext()) {
                Map.Entry<String, String> targetEntry = (Map.Entry)ite.next();
                up.setUpdateParameter(targetEntry.getKey().trim().toLowerCase(), new StringUpdateParameter(targetEntry.getValue()));
            }

            // Where
            settingWhereParameter(tableInfo, up, whereList);

            // クエリ実行
            long queryStartTime = System.nanoTime();
            int resultCount = FemtoHttpServer.dataAccessor.updateTableData(up, tansactionNo);
            long queryEndTime = System.nanoTime();

            // TransactionNo指定なしの場合ここでCommit
            if (onceTransaction) {
                FemtoHttpServer.dataAccessor.commitTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
                tansactionNo = -1L;
            }

            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("application/json; charset=utf-8");
            response.getWriter().print("{\"result\":" + resultCount + "}");
            response.getWriter().flush();
            return;
        } catch (Exception e) {
            e.printStackTrace();
            response.setStatus(500);
        } finally {
            if (onceTransaction && tansactionNo != -1L) {
                FemtoHttpServer.dataAccessor.rollbackTransaction(tansactionNo);
                FemtoHttpServer.dataAccessor.endTransaction(tansactionNo);
            }
        }
    }
}
