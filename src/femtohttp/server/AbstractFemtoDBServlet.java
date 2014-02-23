package femtohttp.server;import java.util.*;import java.io.IOException;import net.arnx.jsonic.JSON; import javax.servlet.ServletException;import javax.servlet.http.HttpServlet;import javax.servlet.http.HttpServletRequest;import javax.servlet.http.HttpServletResponse;import femtodb.core.*;import femtodb.core.table.*;import femtodb.core.table.transaction.*;import femtodb.core.table.data.*;import femtodb.core.table.type.*;import femtodb.core.accessor.*;import femtodb.core.accessor.parameter.*;abstract class AbstractFemtoDBServlet extends HttpServlet {    protected static boolean debug = true;    protected long getTransactionNo() {        TransactionNo tn = FemtoHttpServer.dataAccessor.createTransaction();        return tn.getTransactionNo();    }    protected void settingWhereParameter(TableInfo tableInfo, SelectParameter sp, String[] whereList) {        if (whereList != null) {            for (int idx = 0; idx < whereList.length; idx++) {                if (whereList[idx].indexOf(" = ") != -1) {                    String[] whereDetail = whereList[idx].split(" = ");                            String columneName = whereDetail[0].trim().toLowerCase();                    // 定義済みIndexカラムか通常のカラムの判断                    if (tableInfo.existColumn(columneName)) {                        // 定義済みIndex                        if (debug) System.out.println("where - index '=' Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");                        sp.setIndexWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));                    } else {                        if (debug) System.out.println("where - normal '=' Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");                        sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_EQUAL, new EqualWhereParameter(whereDetail[1]));                    }                 } else if (whereList[idx].indexOf(" index ") != -1) {                    String[] whereDetail = whereList[idx].split(" index ");                            String columneName = whereDetail[0].trim().toLowerCase();                    if (debug) System.out.println("where - normal 'index' Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");                    sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_LIKE, new LikeWhereParameter(whereDetail[1]));                } else if (whereList[idx].indexOf(" < ") != -1) {                    String[] whereDetail = whereList[idx].split(" < ");                            String columneName = whereDetail[0].trim().toLowerCase();                    if (debug) System.out.println("where - normal '<' Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");                    sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_SMALL, new SmallWhereParameter(whereDetail[1]));                } else if (whereList[idx].indexOf(" > ") != -1) {                    String[] whereDetail = whereList[idx].split(" > ");                    String columneName = whereDetail[0].trim().toLowerCase();                    if (debug) System.out.println("where - normal '>' Parameter: column=[" + columneName + "] parameter=[" +  whereDetail[1] + "]");                    sp.addNormalWhereParameter(columneName, IWhereType.WHERE_TYPE_LARGE, new LargeWhereParameter(whereDetail[1]));                }            }        }    }    protected void settingLimitParameter(SelectParameter sp, String limitStr) {        if (limitStr != null) {            sp.setLimit(new Integer(limitStr).intValue());        }    }        protected void settingOffsetParameter(SelectParameter sp, String offsetStr) {        if (offsetStr != null) {            sp.setOffset(new Integer(offsetStr).intValue());        }    }        protected void settingOrderbyParameter(SelectParameter sp, String orderbyListStr) {        if (orderbyListStr != null) {            String[] orderbyList = orderbyListStr.split(",");            for (int idx = 0; idx < orderbyList.length; idx++) {                String orderbyStr = orderbyList[idx];                String[] orderbyChk = orderbyStr.split(" ");                // asc desc の確定                int type = 1;                String typeStr = orderbyChk[1].trim().toLowerCase();                if(!typeStr.equals("asc")) type = 2;                // 文字ソート、数値ソート の確定                if (orderbyChk.length ==3) {                                    sp.addSortParameter(orderbyChk[0].trim().toLowerCase(), type, true);                } else {                    sp.addSortParameter(orderbyChk[0].trim().toLowerCase(), type, false);                }            }        }    }    protected boolean executeTransactionNoValidate(HttpServletResponse response, String transactionNoStr) throws ServletException, IOException {        try {            long tansactionNo = -1L;            if (transactionNoStr != null && !transactionNoStr.trim().equals("")) {                // TransactionNoが指定されている可能性                // 数値チェック                try {                    Long tnNoChk = new Long(transactionNoStr);                    tansactionNo = tnNoChk.longValue();                                        if (tansactionNo < 0) {                        response.setContentType("text/html");                        response.setStatus(400);                        response.getWriter().println("'transactionno' Can specify only positive number.");                        return false;                    }                    // TransactionNoの存在確認                    if (!FemtoHttpServer.dataAccessor.existsTransactionNo(tansactionNo)) {                        response.setContentType("text/html");                        response.setStatus(400);                        response.getWriter().println("The 'transactionno'  specified does not exist");                        return false;                    }                } catch (NumberFormatException nfe) {                    // テーブル名なし                    response.setContentType("text/html");                    response.setStatus(400);                    response.getWriter().println("'transactionno' Can specify only numbers.");                    return false;                }            }        } catch (Exception e) {            throw e;        }        return true;    }    protected boolean executeWhereValidate(HttpServletResponse response, String[] whereList) throws ServletException, IOException {        try {            // カラム名=値 or カラム名 index 値 or カラム名>値 or カラム名<値　のフォーマットである必要がある            for (int idx = 0; idx < whereList.length; idx++) {                String whereDt = whereList[idx];                if (whereDt == null || whereDt.trim().equals("")) {                    response.setContentType("text/html");                    response.setStatus(400);                    response.getWriter().println("'where' Format violation.");                    return false;                } else {                    if (whereDt.indexOf(" = ") != -1) {                        String[] checkWork = whereDt.split(" = ");                        if (checkWork.length < 2) {                            response.setContentType("text/html");                            response.setStatus(400);                            response.getWriter().println("'where' Format violation.");                            return false;                        }                    } else if (whereDt.indexOf(" index ") != -1) {                        String[] checkWork = whereDt.split(" index ");                        if (checkWork.length < 2) {                            response.setContentType("text/html");                            response.setStatus(400);                            response.getWriter().println("'where' Format violation.");                            return false;                        }                    } else if (whereDt.indexOf(" > ") != -1) {                        String[] checkWork = whereDt.split(" > ");                        if (checkWork.length < 2) {                            response.setContentType("text/html");                            response.setStatus(400);                            response.getWriter().println("'where' Format violation.");                            return false;                        }                    } else if (whereDt.indexOf(" < ") != -1) {                        String[] checkWork = whereDt.split(" < ");                        if (checkWork.length < 2) {                            response.setContentType("text/html");                            response.setStatus(400);                            response.getWriter().println("'where' Format violation.");                            return false;                        }                    }                }            }        } catch (Exception e) {            throw e;        }        return true;    }    protected boolean executeLimitValidate(HttpServletResponse response, String limitStr) throws ServletException, IOException {        // limitは正数である必要がある        try {            new Integer(limitStr);        } catch (NumberFormatException e) {            response.setContentType("text/html");            response.setStatus(400);            response.getWriter().println("'limit' Format violation.");            return false;        }        return true;    }    protected boolean executeOffsetValidate(HttpServletResponse response, String offsetStr) throws ServletException, IOException {        // offsetは正数である必要がある        try {            new Integer(offsetStr);        } catch (NumberFormatException e) {            response.setContentType("text/html");            response.setStatus(400);            response.getWriter().println("'offset' Format violation.");            return false;        }        return true;    }    protected boolean executeDataValidateAndConvert(HttpServletResponse response, String[] dataList, List convertStoreList) throws ServletException, IOException {        // 登録データをJSON形式からMapへ変換        try {            for (int idx = 0; idx < dataList.length; idx++) {                Map<String, String> targetData = (Map<String, String>)JSON.decode(dataList[idx]);                convertStoreList.add(targetData);            }        } catch (Exception e) {            response.setContentType("text/html");            response.setStatus(400);            response.getWriter().println("'data' Format violation.");            return false;        }        return true;    }    /**     * "orderby" : ソートカラムと昇順、降順の指定。カラム名と指定を" "で連結<br>     *             並び順は辞書順である.<br>     *             カラムの値が数値もしくはnullもしくは空白であることが保証出来る場合は指定の後方に"+number"と付けることで数値ソートが可能である.<br>     *  例1)"orderby":column3+asc<br>     *  例2)"orderby":column4+asc+number<br>     */    protected boolean executeOrderbyValidate(HttpServletResponse response, String orderbyListStr) throws ServletException, IOException {        // orderbyはカラム名+asc or desc + number(任意)指定        try {            String[] orderbyList = orderbyListStr.split(",");            for (int idx = 0; idx < orderbyList.length; idx++) {                String orderbyStr = orderbyList[idx];                if (orderbyStr.indexOf(" ") == -1) throw new Exception("1");                    String[] orderbyChk = orderbyStr.split(" ");                if (orderbyChk.length == 2 || orderbyChk.length == 3) {                        String type = orderbyChk[1].trim().toLowerCase();                    if (type.equals("asc") || type.equals("desc")) {                        if (orderbyChk.length ==3) {                            if (!orderbyChk[2].trim().toLowerCase().equals("number")) {                                throw new Exception("2");                            }                        }                    } else {                        throw new Exception("3");                    }                } else {                    throw new Exception("4");                }            }        } catch (Exception e) {            response.setContentType("text/html");            response.setStatus(400);            response.getWriter().println("'orderby' Format violation. error cdode=" + e.getMessage());            return false;        }        return true;    }}