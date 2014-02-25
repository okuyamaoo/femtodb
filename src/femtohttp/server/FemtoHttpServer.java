package femtohttp.server;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.servlet.ServletHandler;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.transaction.*;
import femtodb.core.table.data.*;
import femtodb.core.table.type.*;
import femtodb.core.accessor.*;
import femtodb.core.accessor.parameter.*;


/** 
 * FemtoDBをサーバとして起動しHttpプロトコルにてアクセス可能とするクラス<br>
 * インターフェースとしてJettyを利用している<br>
 * サーバモードで利用した場合であってもトランザクション処理が可能である<br>
 *<br>
 * 起動方法例<br>
 * java -server -Xmx2048m -Xms2048m -classpath ./:./classes/:./lib/* femtohttp.server.FemtoHttpServer<br>
 * 起動することでポート番号8080番にてHTTPサーバが起動する<br>
 * アクセスURIは以下となる。<br>
 * <br>
 * テーブル作成.<br>
 * URI:http://localhost:8080/femtodb/table<br>
 * HTTPMethod:POST<br>
 * パラメータ.<br>
 * &nbsp;[必須]<br>
 * &nbsp;&nbsp;table<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;テーブル名を指定<br>
 * &nbsp;[任意]<br>
 * &nbsp;&nbsp;indexcoolumns<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;インデックスを作成したいカラム名(登録JSON内でのKey名)<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;ただし現バージョンでは全て文字インデックスが作成される<br>
 * <br>
 * 返却値.<br>
 * フォーマット:JSON(連想配列形式)<br>
 * Key名:"result" / Value値:"true" or "false"<br>
 * Key名:"errormessage" / Value値:"メッセージ"<br>
 * <br>
 * トランザクションを新規で開始.<br>
 * URI:http://localhost:8080/femtodb/transaction<br>
 * HTTPMethod:GET<br>
 * パラメータ.<br>
 * なし<br>
 * <br>
 * 返却値.<br>
 * フォーマット:JSON(連想配列形式)<br>
 * Key名:"transactionno" / Value値:トランザクション番号(数値)<br>
  * <br>
 * トランザクションをコミットもしくはロールバックする.<br>
 * URI:http://localhost:8080/femtodb/transaction<br>
 * HTTPMethod:POST<br>
 * パラメータ.<br>
 * &nbsp;[必須]<br>
 * &nbsp;&nbsp;transactionno<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;対象とするトランザクションの番号。トランザクション新規開始メソッドで取得した番号を指定<br>
 * &nbsp;&nbsp;method<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;(commit or rollback) 実行したい処理を2つのうちいづれか指定<br>
 * 返却値.<br>
 * フォーマット:JSON(連想配列形式)<br>
 * Key名:"result" / Value値:("true" or "false") 処理の成否<br>
 * <br>
 * トランザクションを終了する.<br>
 * URI:http://localhost:8080/femtodb/transaction<br>
 * HTTPMethod:DELETE<br>
 * パラメータ.<br>
 * &nbsp;[必須]<br>
 * &nbsp;&nbsp;transactionno<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;対象とするトランザクションの番号。トランザクション新規開始メソッドで取得した番号を指定<br>
 * 返却値.<br>
 * フォーマット:JSON(連想配列形式)<br>
 * Key名:"result" / Value値:("true" or "false") 処理の成否<br>
 * <br>
 * テーブル内データを取得する.<br>
 * 条件を指定して取得も可能である<br>
 * URI:http://localhost:8080/femtodb/dataaccess<br>
 * HTTPMethod:GET<br>
 * パラメータ.<br>
 * &nbsp;[必須]<br>
 * &nbsp;&nbsp;table<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;検索を行うテーブル名を指定する<br>
 * &nbsp;[任意]<br>
 * &nbsp;&nbsp;transactionno<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;予め取得したトランザクション番号<br>
 * &nbsp;&nbsp;where
 * &nbsp;&nbsp;&nbsp;&nbsp;検索条　複数指定可能、複数指定し配列とする<br>
 * &nbsp;&nbsp;指定可能な条件は以下<br>
 * &nbsp;&nbsp;"&nbsp;&nbsp;=&nbsp;&nbsp;":左辺のカラムのデータが右辺で指定したデータと完全一致<br>
 * &nbsp;&nbsp;"&nbsp;&nbsp;index&nbsp;&nbsp;":左辺のカラムのデータ内に右辺で指定した文字列でテキスト検索<br>
 * &nbsp;&nbsp;指定条件の"="や"text"の前後には必ず半角スペースを1つづつ入れる.<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;例)where:column1&nbsp;&nbsp;=&nbsp;&nbsp;abc<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;where:column2&nbsp;&nbsp;=&nbsp;&nbsp;201401<br>
 * &nbsp;&nbsp;limit<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;取得件数(数値※Integerの最大値)<br>
 * &nbsp;&nbsp;offset<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;取得開始位置(1始まり。指定した位置のデータを含む)<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;例)データが10件ある場合、offset:2と指定した場合2件目から全件取得となる<br>
 * &nbsp;&nbsp;orderby<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;ソートカラムと昇順、降順の指定。カラム名と指定を" "で連結<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;並び順は辞書順である<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;カラムの値が数値もしくはnullもしくは空白であることが保証出来る場合は指定の後方に"&nbsp;&nbsp;number"と付けることで数値ソートが可能である<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;sort文に関してもカラム名と並び順指定と数値指定の間に半角スペースを1つづつ入れる.<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;例1)"sort":column3&nbsp;&nbsp;asc<br>
 * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;例2)"sort":column4&nbsp;&nbsp;asc&nbsp;&nbsp;number<br>
 * 返却値.<br>
 * フォーマット:JSON(List<Map> リスト形式で内部要素が連想配列)<br>
 * List[Map{"カラム名":"データ", "カラム名":"データ","カラム名":"データ"}, Map{"カラム名":"データ", "カラム名":"データ","カラム名":"データ"}....]<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class FemtoHttpServer {
    
    static DataAccessor dataAccessor = null;
    
    public static void main(String[] args) {
        try {
            // 起動引数をコンパイル
            FemtoDBConstants.build(args);
            FemtoHttpServer femtoHttpServer = new FemtoHttpServer();
            femtoHttpServer.startServer(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void startServer(String[] args) throws Exception {

        // DataAccessorを初期化
        FemtoHttpServer.dataAccessor = new DataAccessor();

        QueuedThreadPool threadPool = new QueuedThreadPool();
        threadPool.setMaxThreads(FemtoDBConstants.HTTP_SERVER_MAXTHREADS);
        Server server = new Server(threadPool);

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);
        handler.addServletWithMapping(femtohttp.server.FemtoDBConnectorTransaction.class, "/femtodb/transaction");
        handler.addServletWithMapping(femtohttp.server.FemtoDBConnectorTransactionList.class, "/femtodb/transactionlist");
        handler.addServletWithMapping(femtohttp.server.FemtoDBConnectorTable.class, "/femtodb/table");
        handler.addServletWithMapping(femtohttp.server.FemtoDBConnectorDataaccess.class, "/femtodb/dataaccess");

        ServerConnector http = new ServerConnector(server);
        http.setPort(FemtoDBConstants.HTTP_SERVER_PORT);
        http.setIdleTimeout(FemtoDBConstants.HTTP_SERVER_IDLETIMEOUT);
        server.addConnector(http);
        server.start();
        server.join();
    }


}