FemtoDB is simple schemaless database

Support table. Support transaction. Support index column. Support column text search.


FemtoDBはトランザクションをサポートしたシンプルなオンメモリのドキュメント型データベースです
カラム構成が不規則な連想配列をJSONフォーマット化したデータを透過的に登録、更新、削除、検索が可能です
アクセスのインターフェースとしてはHTTPでのRestfulAPIとなっています

MVCCモデルを参考したトランザクションが実装されており、全てのデータアクセスは
トランザクション内の閉じた範囲でアトミックに実行、決定、取り消しができます

検索機能を備えており、値の完全一致、テキストサーチ、数値を対象として大小比較を全ての
データにおこなうことが出来ます。
完全一致とテキストサーチに関しては検索インデックスを定義することが可能です
また、ソート機能、limit機能、offset機能を備えておりSQLを意識した構成としました

操作ログ方式によりデータの永続化が行われます。
※デフォルトの起動モードでは永続化機能はoffとなっているため、起動引数の「永続化ログの出力指定」を
参照してください。


[データ操作方式]
以下を備えています
1.テーブル作成
  データを格納するグループとなるテーブルを作成します
  カラム定義は必要ではありませんが、同時に検索を高速化するインデックスを定義することが可能です
  インデックスを作成した場合登録されるデータに作成したインデックスカラム名が存在した場合自動的に作成されます。
  テキストサーチ用のインデックスは内部的にOSS形態素解析器であるkuromojiを利用し構文解析された
  転置インデックスが用いられます

2.トランザクション機能
  データへのアクセス処理を全てトランザクションで保護した範囲内で実現可能です。

3.データ登録
  作成したテーブルにはKey=文字列型、Value=文字列型をの集合となる連想配列をJSONにてフォーマットしたデータを
  登録可能です。このKeyの部分を検索時に指定し、Valueも部分に検索条件を適応します。

4.検索機能
  テーブル内に格納されたデータはどのような連想配列形式であってもKey=文字列型, Value=文字列型であれば検索可能です。
  また、検索結果はデータの持つ要素の値を使いソート可能です。取得範囲を限定するlimit/offsetも利用可能です。

5.データ更新/削除
  検索機能と同じ条件指定にて範囲を絞って更新、削除が可能です。


[永続化機能]
全ての操作はログ方式で記録されディスクに永続化することが可能です。
そのため、FemtoDBを停止した場合であっても起動時にログファイルが残っている場合
自動的に最後の動作部分までデータが復元されます。


[必要なソフトウェア]
 java version "1.7"以上
 kuromoji(Version-0.7.7)  http://www.atilika.org/
jsonic(Version-01.3.1) http://jsonic.sourceforge.jp/
jetty(Version-9.1.1) http://www.eclipse.org/jetty/
※上記は全て同梱しています。


[起動]
・インストール
  $unzip femtodb-0.0.1.zip
・起動
  展開したディレクトリに移動
  $java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer

起動後ポート番号8080にてアクセス可能
詳しい利用方法はFemtoDB-HTTPMethod-List.txtを参照してください

[起動引数(オプション)]
"-項目名 値"のフォーマットで指定

・永続化ログの出力指定
  -tlw true/false
   "true"出力する
   "false"出力しない
   デフォルト:false
 TODO:永続化機能を有効にした場合、起動時に停止前の状態に自動的にデータが復元されます
            現バージョンではログへの追記方式をとっているため、ログファイルの肥大化問題を持っているため、
            稼働サーバのディスク利用量に注意してください。

・永続化ログの出力ファイル名
  -tl ファイルパス
   ファイル名をフルパスもしくは相対パスで指定
   デフォルト:./femtodb.log
  設定例) -tl ./femtodb.log

・FemtoDBのHTTPサーバの起動ポート
  -httpport 数値
   ポート番号を指定
   デフォルト:8080
  設定例) -httpport 8088


・FemtoDBのHTTPサーバ同時接続最大数
  -maxclients 数値
   最大数数値で指定
   デフォルト:150
  設定例) -maxclients 200

・FemtoDBの応答タイムアウト時間
  -timeout 数値
   最大時間をミリ秒で指定
   デフォルト:30000
  設定例) -timeout 45000

・FemtoDBの内蔵ストレージモード
  -storage serialize
   "serialize"を指定すると速度は落ちるがメモリを節約出来る
   デフォルト:高速ストレージ
   ※永続化ログを出力している場合ストレージを変えた場合で合っても互換性が保たれデータは復元される
  設定例) -storage serialize

・インデクッスを利用したクエリの並列実行数
  -iqp 数値
    大きくし過ぎるとCPUを専有してしまうためCPU数の1/2程度を推奨
   デフォルト:4
  設定例) -iqp 6

・インデクッスを利用しないクエリの並列実行数
  -nqp 数値
    大きくし過ぎるとCPUを専有してしまうためCPU数の1/4程度を推奨
    インデックスを利用しない検索クエリをほとんど利用しない場合は1などでも良い
   デフォルト:2
  設定例) -nqp 1

・全件取得クエリの並列実行数
  -fqp 数値
    大きくし過ぎるとCPUを専有してしまうためCPU数の1/4程度を推奨
    全件取得検索クエリをほとんど利用しない場合は1などでも良い
   デフォルト:1
  設定例) -fqp 1



設定例)
例1)永続化を行い起動
$java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer -tlw true

例2)起動ポートを7707へ変更し起動
$java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer -httpport 7707

例3)永続化を行いながら利用メモリを節約した起動
$java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer -tlw true -storage serialize

例4)CPU16コアのサーバ上で利用し、インデックスを利用しないクエリを利用しない場合
$java -server -Xmx2048m -Xms2048m -cp ./:./lib/*:./bin/* femtohttp.server.FemtoHttpServer -tlw true -iqp 8 -nqp 1



