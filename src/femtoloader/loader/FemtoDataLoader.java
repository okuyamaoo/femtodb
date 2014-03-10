package femtoloader.loader;

import java.util.*;
import java.io.*;
import java.net.*;
import java.math.BigDecimal;
import net.arnx.jsonic.JSON; 

/**
 * TSVファイルを指定することで指定したFemtoDBへデータを登録します<br>
 * また同時にテーブルを作成することもできます<br> 
 * 利用方法は以下になります.<br>
 * 1.FemtoDBを起動
 *   java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer -tlw true
 *<br>
 * 2.TSVを用意する<br>
 *  カラム間の区切りをタブ文字としたTSVを用意<br>
 *   1行目にカラム情報を指定。指定しない場合は自動的に1列名が"column1"、2列目が"column2"といったように<br>
 *    column数値のカラムがデータの列分作成される。<br>
 *   2行目以降がデータ部分となる。<br>
 *    文字コードは全てUTF-8固定となる。<br>
 *  例-カラム指定なし)
 * 1行目：
 * 2行目：カラム1のデータその1\tカラム2のデータその1\tカラム3のデータその1\tカラム4のデータその1
 * 3行目：カラム1のデータその2\tカラム2のデータその2\tカラム3のデータその2\tカラム4のデータその2
 * 4行目：カラム1のデータその3\tカラム2のデータその3\tカラム3のデータその3\tカラム4のデータその3
 *
 *  例-カラム指定あり)
 * 1行目：originalcolumn1\toriginalcolumn2\toriginalcolumn3\toriginalcolumn4
 * 2行目：カラム1のデータその1\tカラム2のデータその1\tカラム3のデータその1\tカラム4のデータその1
 * 3行目：カラム1のデータその2\tカラム2のデータその2\tカラム3のデータその2\tカラム4のデータその2
 * 4行目：カラム1のデータその3\tカラム2のデータその3\tカラム3のデータその3\tカラム4のデータその3
 *
 *<br>
 * 3.テーブル名、インデックスカラムを決める<br>
 *   既にテーブル作成している場合はそのテーブルを利用可能<br>
 *   検索を行う場合はインデクスを検討<br>
 *<br>
 * 4.実行
 *    java -classpath ./:./bin/*:./lib/* femtoloader.loader.FemtoDataLoader /var/tmp/data.tsv testtable 127.0.0.1 8888 "originalcolumn1:equal" "originalcolumn4:text"
 *
 *  引数説明 
 *  第1引数：取り込み用のTSVファイルのパス
 *  第2引数：取り込み対象のテーブル名(テーブルが存在しない場合は自動作成される。存在する場合は追加登録される)
 *  第3引数：FemtoDBの起動しているサーバのアドレス
 *  第4引数：FemtoDBの起動しているポート番号
 *  第5引数以降：存在しないテーブルを指定した場合、第4引数以降で検索インデックスを指定出来る。
 *             指定フォーマットは[カラム名][コロン][インデックスタイプ]となる
 *
 *             上記の例では"originalcolumn1:equal"の場合
 *             カラム名originalcolumn1に完全一致時(" = "条件)にインデックスである"equal"を作成している
 *
 *             また"originalcolumn2:text"の場合
 *             カラム名originalcolumn2にテキスト検索用(" text "条件)インデックスである"text"を作成している
 *           以降可変長としていくつでも指定出来る。
 *<br>
 * 5.実行結果
 *  実行後実行結果として登録件数が出力されます。
 *  登録に失敗した場合、全てRollbackされる。テーブルを自動作成した場合はそのテーブルも削除されます。
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class FemtoDataLoader {

    public static void main(String[] args) {


        try {
            
            if (args.length < 4) {
                System.out.println("Usage : java -classpath ./:./bin/*:./lib/* femtoloader.loader.FemtoDataLoader filepath tablename FemtoDB-Adress FemtoDB-Port *(index...)");
                System.out.println("      : Tsv format file only.");
                System.out.println("      : Character encoding UTF-8 only.");

                System.exit(1);
            }

            File file = new File(args[0]);
            String tableName = args[1].trim().toLowerCase();

            if (!file.exists()) {
                System.out.println(" Load target file [" + args[0] + "] not found");
                System.exit(1);
            }

            FemtoDataLoader loader = new FemtoDataLoader();
            loader.load(file, tableName, args[2], Integer.parseInt(args[3]), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
     }
     

    public boolean load(File loadFile, String tableName, String address, int port, String[] args) throws Exception {
        boolean createTableExeute = false;

        System.out.println("DataLoad - Start - " + new Date());
        System.out.println(" File check start - " + new Date());
        System.out.println("  Load target file [" + loadFile.getName() + "]");
        System.out.println("   File size [" + loadFile.length() + "]");
        try {
            int loadCnt = 0;
            BufferedReader br = new BufferedReader(new FileReader(loadFile));
            String line = null;
            while ((line = br.readLine()) != null) {
                loadCnt++;
            }
    
            System.out.println("   File lines [" + loadCnt + "]");
            br.close();
    
            if (loadCnt < 2) {
                // ファイルは1行目がカラム名もしくは、空白行でありその後データ行を1行以上含むデータである必要があるので、
                // それ以外のファイルはエラー
                System.out.println(" [ERROR] The format of the file is a blank row or column definition one line, one line of data or more must have at least two subsequent lines.");
                System.exit(1);
            }
            System.out.println(" File check finish - " + new Date());
    
    
            System.out.println(" Load target table check start - " + new Date());
            // テーブル存在確認
            if (!existsTable(address, port, tableName)) {
                System.out.println("  Load target table not found");
                System.out.println("   Table create start ");
    
                createTableExeute = true;
                // テーブル自動作成
                if (createTable(address, port, tableName, args)) {
                    System.out.println("    Create table [" + tableName +"] Success");
                } else {
                    System.out.println(" [ERROR] Create table [" + tableName +"] Fail");
                    System.exit(1);
                }
            }
            System.out.println(" Load target table check finish - " + new Date());
    
            // データロードスタート
            System.out.println(" Data load start - " + new Date());
            // 1行目が空白行の判定
            boolean noColumn = true;
            String[] columnList = null;
    
            br = new BufferedReader(new FileReader(loadFile));
            String firstLine = br.readLine();
    
            if (!firstLine.trim().equals("")) {
                noColumn = false;
                columnList = firstLine.split("\t");
            }
    
            line = null;
            int successCnt = 0;
            int errorCnt = 0;
            long tno = getTransactionNo(address, port);
            while ((line = br.readLine()) != null) {
    
                if (successCnt != 0 && (successCnt % 5000) == 0) System.out.println("  " + new Date() + " -- Load line number " + successCnt);
                String[] data = line.split("\t");
    
                if (noColumn) {
    
                    // カラム定義なし
                    // カラム名は先頭からcolumn1, column2, column3・・・といったように順に作成される
                    Map insertData = new LinkedHashMap();
                    for (int idx = 0; idx < data.length; idx++) {
                        insertData.put("column"+(idx+1), data[idx]);
                    }
                    int ret = insert(address, tableName, tno, port, insertData);
                    if (ret == -1) {
    
                        System.out.println("  insert error -[" + line + "]");
                        if (rollbackTransaction(address, port, tno)) {
                            System.out.println("   [ERROR] Rollback transaction - executed");
                            if (createTableExeute) removeTable(address, port, tableName);
                            System.exit(1);
                        } else {
                            System.out.println("  [ ERROR] Rollback transaction - fail. see console log");
                            if (createTableExeute) removeTable(address, port, tableName);
                            System.exit(1);
                        }
                    } else {
                        successCnt++;
                    }
                } else {
                    // カラム定義あり
                    // カラム名は先頭行の定義を先頭から適応する。カラム定義数を超えるデータは無視される
                    Map insertData = new LinkedHashMap();
                    for (int idx = 0; idx < columnList.length; idx++) {
                        if (idx < data.length) {
                            insertData.put(columnList[idx], data[idx]);
                        }
                    }
                    int ret = insert(address, tableName, tno, port, insertData);
                    if (ret == -1) {
    
                        System.out.println("  [ERROR] insert error -[" + line + "]");
                        if (rollbackTransaction(address, port, tno)) {
                            System.out.println("   [ERROR] Rollback transaction - executed");
                            if (createTableExeute) removeTable(address, port, tableName);
                            System.exit(1);
                        } else {
                            System.out.println("   [ERROR] Rollback transaction - fail. see console log");
                            if (createTableExeute) removeTable(address, port, tableName);
                            System.exit(1);
                        }
                    } else {
                        successCnt++;
                    }
                }
            }
            if (commitTransaction(address, port, tno)) {
                System.out.println("  Commit transaction - executed");
                System.out.println("   Loaded data count =[" + successCnt + "]");
            } else {
                System.out.println("  [EEROR] Commit transaction - fail. see console log");
                if (createTableExeute) removeTable(address, port, tableName);
                System.exit(1);
            }
            System.out.println(" Data load finish - " + new Date());
            System.out.println("DataLoader - finish - " + new Date());
            return true;
        } catch (Exception e) {
            if (createTableExeute) removeTable(address, port, tableName);
            throw e;
        }
    }

    private long getTransactionNo(String address, int port) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/transaction";
        String result = executeHttpProcess(url, "GET", "");
        Map ret = (Map)JSON.decode(result);
        BigDecimal no = (BigDecimal)ret.get("transactionno");
        return no.longValue();
    }

    private boolean commitTransaction(String address, int port, long tno) throws Exception {
        String url = "http://" + address + ":" + port + "/femtodb/transaction";
        String result = executeHttpProcess(url, "POST", "transactionno=" + tno + "&method=commit");
        if (result == null) {
            return false;
        } else {
            Map ret = (Map)JSON.decode(result);
            String retStr = (String)ret.get("result");
            if (retStr.equals("true")) return true;
        }
        return false;
    }

    private boolean rollbackTransaction(String address, int port, long tno) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/transaction";
        String result = executeHttpProcess(url, "POST", "transactionno=" + tno + "&method=commit");
        if (result == null) {
            return false;
        } else {
            Map ret = (Map)JSON.decode(result);
            String retStr = (String)ret.get("result");
            if (retStr.equals("true")) return true;
        }
        return false;
    }


    private int insert(String address, String tableName, long tno, int port, Map insertData) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/dataaccess";
        String result = executeHttpProcess(url, "POST", "transactionno=" + tno + "&table=" + tableName + "&data=" + JSON.encode(insertData));
        if (result == null) return -1;
        Map ret = (Map)JSON.decode(result);
        BigDecimal cnt = (BigDecimal)ret.get("result");
        return cnt.intValue();
    }

    private boolean existsTable(String address, int port, String tableName) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/table";
        String result = executeHttpProcess(url, "GET", "");
        

        if (result == null) throw new Exception("Table list not found");

        Map tableMap = (Map)JSON.decode(result);
        Object retObj = tableMap.get(tableName.toLowerCase());

        if (retObj != null) return true;
        return false;
    }

    private boolean createTable(String address, int port, String tableName, String[] indexList) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/table";

        StringBuilder indexColumns = new StringBuilder("");
        String sep ="";
        for (int i = 4; i < indexList.length; i++) {
            indexColumns.append(sep);
            String[] indexDt = indexList[i].split(":");
            if (indexDt.length != 2) throw new Exception("Create index format violation.");
            if (!indexDt[1].equals("equal") && !indexDt[1].equals("text")) throw new Exception(indexDt[1] + " index type not found.");
            
            indexColumns.append(indexDt[0]+":"+indexDt[1]); 
            sep =",";
        }

        String indexConfig = indexColumns.toString();
        if (!indexConfig.equals("")) {
            indexConfig = "&indexcolumns=" + indexConfig;
        }

        String result = executeHttpProcess(url, "POST", "table=" + tableName.toLowerCase() + indexConfig);
        if (result == null) throw new Exception("Table create fail");

        Map createResult = (Map)JSON.decode(result);
        String ret = (String)createResult.get("result");
        if (ret.equals("true")) return true;
        return false;
    }

    private boolean removeTable(String address, int port, String tableName) throws Exception {
        System.out.println("  RemoveTable - start ");
        System.out.println("   Table name [" + tableName +"]");

        String url = "http://" + address + ":" + port + "/femtodb/table?table=" + tableName;
        String result = executeHttpProcess(url, "DELETE", "");

        if (result == null) throw new Exception("RemoveTable error");

        Map deleteRet = (Map)JSON.decode(result);
        String resultStr = (String)deleteRet.get("result");
        if (resultStr != null && resultStr.equals("true")) {
            System.out.println("   RemoveTable - Success ");
            System.out.println("  RemoveTable - finish");
            return true;
        }
        System.out.println("   RemoveTable - Fail ");
        System.out.println("  RemoveTable - finish");
        return false;
    }



    private String getTransactionNoList(String address, int port) throws Exception {

        String url = "http://" + address + ":" + port + "/femtodb/transactionlist";
        String result = executeHttpProcess(url, "GET", "");
        return result;
    }






    private String executeHttpProcess(String urlStr, String method, String sendContent) throws Exception {

        URL url = new URL(urlStr);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        if (method.equals("POST")) {
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
        } else if (method.equals("GET")) {
            con.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
        } else if (method.equals("DELETE")) {
            con.setDoOutput(true);
            con.setRequestProperty("Content-Type","application/x-www-form-urlencoded");
        }
        con.connect();
        
        if (method.equals("POST")) {

            PrintWriter pw = new PrintWriter(new BufferedWriter(new OutputStreamWriter(con.getOutputStream() ,"utf-8")));
            pw.print(sendContent);
            pw.flush();
            pw.close();
        }

        // 結果確認
        String line = null;
        if (con.getResponseCode() ==200) {
            // 成功 
            BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(),"utf-8"));
            line = br.readLine();
            br.close();
        } else {
            System.out.println("Request fail HTTP status code=[" + con.getResponseCode() + "]");
        }
        con.disconnect(); 
        return line;
    }
}
