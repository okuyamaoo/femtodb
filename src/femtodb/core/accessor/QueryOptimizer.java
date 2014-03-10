package femtodb.core.accessor;import java.util.*;import java.util.concurrent.ConcurrentHashMap;import java.util.concurrent.ArrayBlockingQueue;import java.util.concurrent.atomic.AtomicLong;import femtodb.core.*;import femtodb.core.util.*;import femtodb.core.table.*;import femtodb.core.table.data.*;import femtodb.core.accessor.parameter.*;import femtodb.core.table.index.*;import femtodb.core.table.type.*;import femtodb.core.table.transaction.*;import femtodb.core.accessor.parameter.*;/**  * QueryOptimizerクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class QueryOptimizer {    private static TableManager tableManager = null;    public static Map<String, Long> lastRebuildIndexInfo = new ConcurrentHashMap();    public static Map<String, Long> lastUpdateAccessTimeInfo = new ConcurrentHashMap();    public static Map[] priorityGroupMap = new ConcurrentHashMap[11];    static ThreadGroupContollor threadGroupContollor = null;    // インスタンス化不可    private QueryOptimizer() {    }    // スレッド、クエリーの最適化のためTableManagerを利用    public static void setTableManager(TableManager tableManager) {        QueryOptimizer.tableManager = tableManager;        QueryOptimizer.threadGroupContollor = new ThreadGroupContollor();        QueryOptimizer.threadGroupContollor.setPriority(10);        QueryOptimizer.threadGroupContollor.start();        for (int i = 1; i < 11; i++) {            priorityGroupMap[i] = new ConcurrentHashMap();        }    }    public static boolean checkModifyedTable(String tableName, long startTime) {        Long lastUpdateTime = (Long)lastUpdateAccessTimeInfo.get(tableName);        Long lastRebuildTime = (Long)lastRebuildIndexInfo.get(tableName);        if (lastUpdateTime == null && lastRebuildTime != null && lastRebuildTime.longValue() <  startTime) return true;        if (lastUpdateTime != null && lastRebuildTime == null) return false;        if (lastUpdateTime.longValue() < lastRebuildTime.longValue() && lastRebuildTime.longValue() <  startTime) return true;        return false;    }    public static void removeThreadGroupData(Thread thread) {        try {            int groupPriority = thread.getPriority();            Map groupMap = priorityGroupMap[groupPriority];            groupMap.remove(thread.getName());        } catch (Exception e) {        }    }    // TODO:ここでパラメータとその他情報を使ってIndex等の処理をする    // Select特有の処理をいれる    public static TableIterator execute(TransactionNo tn, TableManager tableManager, String tableName, SelectParameter selectParameter, ITable table) {        return table.getTableDataIterator(tn, selectParameter);    }    // TODO:ここでパラメータとその他情報を使ってIndex等の処理をする    // Update特有の処理をいれる    public static TableIterator execute(TransactionNo tn, TableManager tableManager, String tableName, UpdateParameter updateParameter, ITable table) {        SelectParameter selectParameter = new SelectParameter();        selectParameter.setNormalWhereParameterListObject(updateParameter.getNormalWhereParameterListObject());        selectParameter.setIndexWhereParameterObject(updateParameter.getIndexWhereParameterObject());        return table.getTableDataIterator(tn, selectParameter);    }    // TODO:ここでパラメータとその他情報を使ってIndex等の処理をする    // Delete特有の処理をいれる    public static TableIterator execute(TransactionNo tn, TableManager tableManager, String tableName, DeleteParameter deleteParameter, ITable table) {        SelectParameter selectParameter = new SelectParameter();        selectParameter.setNormalWhereParameterListObject(deleteParameter.getNormalWhereParameterListObject());        selectParameter.setIndexWhereParameterObject(deleteParameter.getIndexWhereParameterObject());        return table.getTableDataIterator(tn, selectParameter);    }    /**     * クエリーを条件指定の有無等で並列実行制御を行う     *     */    public final static Object getParallelsSyncObject(TransactionNo transactionNo, SelectParameter selectParameter, Thread executeThread) {        Object syncObj = null;        if (selectParameter.existIndexWhereParameter()) {            // インデックスを使う条件            long req = FemtoDBConstants.indexRequestCount.incrementAndGet();            int syncPoint = Long.valueOf(req % FemtoDBConstants.indexParallelsNumber).intValue();            boolean prioritySet = false;            // インデックスを利用する場合は基本Thread優先度は高いが、TransactionNo内の変更情報が大きい場合は優先度を下げる            if (transactionNo.modTableFolder != null) {                Map modTableMap = transactionNo.modTableFolder.get(selectParameter.getTableName());                if (modTableMap != null && modTableMap.size() > 5000) {                    // 変更データが多いので優先を落とす                    executeThread.setPriority(3);                    prioritySet = true;                }            }                        if (!prioritySet) {                // 変更データが少ない                // インデックス対象のデータ数を調べる                NormalWhereParameter indexWhereParameter = selectParameter.getIndexWhereParameter();                String indexColumnName = indexWhereParameter.getColumnName();                IWhereParameter indexParameter = indexWhereParameter.getParameter();                    IndexMap indexMap = tableManager.tableDataMap.get(selectParameter.getTableName()).getIndexsMap().get(indexColumnName);                if (indexMap == null) {                    // インデックスのデータが存在しない                    // 結果が0になる                    // 優先最大                    executeThread.setPriority(10);                } else {                    Map indexDataGrp = indexMap.getIndexGroup(indexParameter.toString());                    if (indexDataGrp == null) {                        // インデックスではあるがパラメータ指定にデータがない                        executeThread.setPriority(9);                        prioritySet = true;                    } else {                        int indexDataGrpSize = indexDataGrp.size();                        if (indexDataGrpSize > 150000) {                            // インデックスではあるが対象データが多すぎる                            // 優先度落とす                            executeThread.setPriority(3);                        } else if (indexDataGrpSize > 100000) {                            executeThread.setPriority(4);                        } else if (indexDataGrpSize > 70000) {                            executeThread.setPriority(5);                        } else if (indexDataGrpSize > 50000) {                            executeThread.setPriority(6);                        } else if (indexDataGrpSize > 30000) {                            executeThread.setPriority(8);                        } else {                            executeThread.setPriority(9);                        }                        prioritySet = true;                    }                }            }            syncObj = FemtoDBConstants.indexParallelsSync[syncPoint];        } else if (selectParameter.existNormalWhereParameter()) {            // インデックスを使わない通常の条件            ITable table = tableManager.tableDataMap.get(selectParameter.getTableName());            int recSize = table.getRecodeSize();            if (recSize > 150000) {                // 優先度落とす                executeThread.setPriority(2);            } else if (recSize > 50000) {                executeThread.setPriority(3);            } else if (recSize > 30000) {                if (transactionNo.modTableFolder != null) {                    Map modTableMap = transactionNo.modTableFolder.get(selectParameter.getTableName());                    if (modTableMap != null && modTableMap.size() < 25000) {                        // 変更データが多いので優先を落とす                        executeThread.setPriority(4);                    } else {                        executeThread.setPriority(3);                    }                } else {                    executeThread.setPriority(4);                }            } else {                if (transactionNo.modTableFolder != null) {                    Map modTableMap = transactionNo.modTableFolder.get(selectParameter.getTableName());                    if (modTableMap != null && modTableMap.size() < 25000) {                        // 変更データが多いので優先を落とす                        executeThread.setPriority(4);                    }                } else if (recSize < 10000) {                    executeThread.setPriority(6);                } else {                    executeThread.setPriority(4);                }            }            long req = FemtoDBConstants.parameterRequestCount.incrementAndGet();            int syncPoint = Long.valueOf(req % FemtoDBConstants.parameterParallelsNumber).intValue();            syncObj = FemtoDBConstants.parameterParallelsSync[syncPoint];        } else {            // 条件を全く使わない全件取得            long req = FemtoDBConstants.allSearchRequestCount.incrementAndGet();            int syncPoint = Long.valueOf(req % FemtoDBConstants.allSearchParallelsNumber).intValue();            boolean prioritySet = false;            if (transactionNo.modTableFolder != null) {                Map modTableMap = transactionNo.modTableFolder.get(selectParameter.getTableName());                if (modTableMap != null && modTableMap.size() > 5000) {                    // 変更データが多いので優先を落とす                    executeThread.setPriority(1);                    prioritySet = true;                }            }            if (!prioritySet) {                // テーブル件数に応じて優先度変更                ITable table = tableManager.tableDataMap.get(selectParameter.getTableName());                if (table != null) {                    int recSize = table.getRecodeSize();                    if (recSize < 10000) {                        executeThread.setPriority(8);                    } else if (recSize < 30000) {                        executeThread.setPriority(4);                    } else {                        if (selectParameter.existSortParameter()) {                            executeThread.setPriority(2);                        } else {                            executeThread.setPriority(3);                        }                    }                } else {                    executeThread.setPriority(8);                }            }            syncObj = FemtoDBConstants.allSearchParallelsSync[syncPoint];        }                // 優先度グループに登録        priorityGroupMap[executeThread.getPriority()].put(executeThread.getName(), executeThread);        // 優先度変更実行        threadGroupContollor.putRequestQueue(executeThread);        if (FemtoDBConstants.DB_REQUEST_EXECUTION_PRIORITY_LOG) {            SystemLog.queryPriorityLog("SelectParameter=[" + selectParameter + "] Priority=" + executeThread.getPriority());        }        return syncObj;    }    static class ThreadGroupContollor extends Thread {        // 登録されたThreadの実行優先度を使い、より低い優先度のThreadの実行を一旦止て        // 優先度の高いThreadの実行を早める        ArrayBlockingQueue<Thread> requestQueue = new ArrayBlockingQueue(100000);        public void run() {            while (true) {                try {                    Thread targetJobThread = requestQueue.take();                    int priority = targetJobThread.getPriority();                                        int executeCnt = 0;                    for (int i = 0; i < priority; i++) {                        Map<String, Thread> group = priorityGroupMap[i];                        boolean execute = false;                        if (group != null && group.size() > 0) {                            for (Iterator it = group.entrySet().iterator(); it.hasNext();) {                                Map.Entry<String, Thread> entry = (Map.Entry)it.next();                                Thread yieldTarget = entry.getValue();                                if (yieldTarget != null) {                                    yieldTarget.yield();                                    executeCnt++;                                }                                if (executeCnt > 1) break;                            }                        }                        if (executeCnt > 1) break;                    }                } catch (Exception e) {                    e.printStackTrace();                }            }        }                        public void putRequestQueue(Thread th) {            try {                requestQueue.put(th);            } catch (Exception e) {                e.printStackTrace();            }        }    }}