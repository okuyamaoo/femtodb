package femtodb.core.accessor;


import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import femtodb.core.*;
import femtodb.core.util.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;
import femtodb.core.accessor.scripts.*;


/** 
 * SelectTableAccessorクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class SelectTableAccessor {


    TableManager tableManager = null;

    public SelectTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public ResultStruct select(SelectParameter selectParameter, TransactionNo transactionNo) throws SelectException {

        List resultList = null;
        int baseResultCount = 0;

        Thread th = Thread.currentThread();
        Object syncObj = QueryOptimizer.getParallelsSyncObject(transactionNo, selectParameter, th);

        try {
            synchronized(syncObj) {
    
                long sec1Start = System.nanoTime();
    
                if (selectParameter.existNormalWhereParameter() || selectParameter.existIndexWhereParameter()) {
                    // 検索条件有り
                    resultList = select(selectParameter.getTableName(), selectParameter, transactionNo);
                } else {
                
                    // 全件取得
                    resultList = select(selectParameter.getTableName(), transactionNo);
                }
        
                long sec1End = System.nanoTime();
                SystemLog.println("select - section1 time=" + (sec1End - sec1Start));
    
        
                // limit offset 前の件数
                baseResultCount = resultList.size();
        
                long sec2Start = System.nanoTime();
                // order by は簡易実装
                if (selectParameter.existSortParameter()) {
                    resultList = orderBy(resultList, selectParameter);
                    //resultList = limitOffset(resultList, selectParameter);
                } else {
                    // Limit Offset は簡易実装
                    resultList = limitOffset(resultList, selectParameter);
                }
                long sec2End = System.nanoTime();
                SystemLog.println("select - section2 time=" + (sec2End - sec2Start));
            }
    
            // 結果のフォルダー
            ResultStruct resultStruct = new ResultStruct(baseResultCount, resultList);
    
            return resultStruct;
        } catch (Exception e) {
            throw new SelectException(e);
        } finally {
            QueryOptimizer.removeThreadGroupData(th);
        }
    }

    /**
     * 全件取得
     *
     *
     */
    protected List<TableDataTransfer> select(String tableName, TransactionNo transactionNo) {
        ITable table = this.tableManager.getTableData(tableName);
        List<TableDataTransfer> allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());

        long start = System.nanoTime();
        TableIterator iterator = table.getTableDataIterator();
        while (iterator.hasNext()) {

            iterator.next();
            TableData tableData = (TableData)iterator.getEntryValue();
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
            if (tableDataTransfer != null) {
                allData.add(tableDataTransfer);
            }
        }
        long end = System.nanoTime();
        SystemLog.println("select - all time=" + (end - start));

        return allData;
    }


    /**
     * 条件適応
     *
     *
     */
    protected List<TableDataTransfer> select(String tableName, SelectParameter selectParameter, TransactionNo transactionNo) throws Exception {
        ITable table = this.tableManager.getTableData(tableName);
        List<TableDataTransfer> allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());

        // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
        TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, selectParameter, table);

        NormalWhereParameter normalWhereParameter = selectParameter.nextNormalWhereParameter();
        NormalWhereExecutor normalWhereExecutor = null;
        if (normalWhereParameter != null) {
            try {
                normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
            } catch (Exception e) {
                throw e;
            }
        }

        if (normalWhereParameter != null) {
            if (transactionNo.modTableFolder != null && transactionNo.modTableFolder.containsKey(tableName)) {

                normalWhereExecutor.execute(iterator, transactionNo, allData);
            } else if (!QueryOptimizer.checkModifyedTable(tableName, System.nanoTime())) { 

                normalWhereExecutor.execute(iterator, transactionNo, allData);
            } else if (!selectParameter.existIndexWhereParameter()) {

                normalWhereExecutor.execute(iterator, transactionNo, allData);
            } else if (selectParameter.existIndexWhereParameter()) {
                while (iterator.hasNext()) {
        
                    iterator.next();
                    TableData tableData = (TableData)iterator.getEntryValue();
                    TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
                    if (tableDataTransfer != null) {
                        allData.add(tableDataTransfer);
                    }

                    if (!QueryOptimizer.checkModifyedTable(tableName, System.nanoTime())) { 
                        // データを収集中にデータ追加が動きIndexが変わってしまっている。
                        allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());
    
                        iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, selectParameter, table);
                        normalWhereExecutor.execute(iterator, transactionNo, allData);
                    }
                }
            }
        } else {
            while (iterator.hasNext()) {
        
                iterator.next();
                TableData tableData = (TableData)iterator.getEntryValue();
                TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
                if (tableDataTransfer != null) {
                
                    allData.add(tableDataTransfer);
                }
            }
        }

        while ((normalWhereParameter = selectParameter.nextNormalWhereParameter()) != null) {


            normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));

            int allDataSize = allData.size();

            if (allDataSize > 0) {

                List<TableDataTransfer> tmpAllData = new ArrayList(allDataSize);

                for (TableDataTransfer targetTableDataTransfer : allData) {

                    if (normalWhereExecutor.execute(targetTableDataTransfer)) {
                        tmpAllData.add(targetTableDataTransfer);
                    }
                }
                allData = tmpAllData;
           }
        }
        return allData;
    }

    /**
     * Order byは簡易実装.<br>
     * TODO:Indexを使う予定<br>
     * TODO:limit offset と合わせて今後修正<br>
     */
    private List<TableDataTransfer> orderBy(List<TableDataTransfer> resultList, SelectParameter selectParameter) {
        try {
            if(resultList != null && selectParameter.existSortParameter()) {

                // Limit offsetを同時に実行する
                DataSortComparator dataSortComparator = new DataSortComparator(selectParameter.getSortParameterList());
                int resultListSize = resultList.size();

                if (resultListSize > 1000) {

                    int splitSize = 400;
                    if (resultListSize < 5000) {
                        splitSize = 10;
                    } else if (resultListSize < 10000) {
                        splitSize = 16;
                    }

                    int limitOffsetSettingPattern = limitOffsetSettingPattern(selectParameter);
                    int[] limitOffsetIndexs = createLimitOffsetPosition(limitOffsetSettingPattern, resultListSize, selectParameter);

                    List<SortParameter> list = selectParameter.getSortParameterList();
                    SortParameter firstSortParameter = list.get(0);
                    int preSortListSize = resultListSize / splitSize;
                    int samplePointBase = resultListSize / preSortListSize;

                    TreeMap preSortMap = new TreeMap();

                    List tailList = new ArrayList(1000);
                    List nullDataList = new ArrayList(1000);
                    long start1 = System.nanoTime();
                    for (int i = 0; i < preSortListSize; i++) {
                        String colVar = resultList.get(samplePointBase*i).getColumnData(firstSortParameter.columnName);
                        if (colVar != null) {
                            if (firstSortParameter.numberSort) {
                                preSortMap.put(new Double(colVar), new ArrayList(preSortListSize*2));
                            } else {
                                preSortMap.put(colVar, new ArrayList(preSortListSize*2));
                            }
                        }
                    }

                    //long end1 = System.nanoTime();
                    //SystemLog.println("time1=" + (end1 - start1) + " preSortMapSIze=" + preSortMap.size());
                    //long start2 = System.nanoTime();
                    for (TableDataTransfer tableDataTransfer:resultList) {
                        String colVar = tableDataTransfer.getColumnData(firstSortParameter.columnName);

                        if (colVar != null) {

                            Map.Entry preSortGroupEntry = null;
                            if (firstSortParameter.numberSort) {

                                preSortGroupEntry = preSortMap.ceilingEntry(new Double(colVar));
                            } else {

                                preSortGroupEntry = preSortMap.ceilingEntry(colVar);
                            }

                            if (preSortGroupEntry != null) {

                                List preSortGroup = (List)preSortGroupEntry.getValue();
                                preSortGroup.add(tableDataTransfer);
                            } else {
                                tailList.add(tableDataTransfer);
                            }
                        } else {
                            nullDataList.add(tableDataTransfer);
                        }
                    }
                    //long end2 = System.nanoTime();
                    //SystemLog.println("time2=" + (end2 - start2));

                    //long start3 = System.nanoTime();

                    resultList = new ArrayList(resultListSize);
                    int totalSortTargetCnt = 0;
                    boolean limitOffsetAssist = false;

                    // ASC　と　DESCによりPreSortMapを回転させる始点が変わる
                    if (firstSortParameter.type == 1) {
                        // asc
                        for (Iterator ite = preSortMap.entrySet().iterator(); ite.hasNext();) {
                            Map.Entry entry = (Map.Entry)ite.next();

                            List preSortGroup = (List)entry.getValue();
                            totalSortTargetCnt = totalSortTargetCnt + preSortGroup.size();

                            if (limitOffsetSettingPattern != -1 && totalSortTargetCnt > limitOffsetIndexs[0]) {
                                if (limitOffsetAssist == true && totalSortTargetCnt > (limitOffsetIndexs[0] + limitOffsetIndexs[1])) {
                                    limitOffsetAssist = true;
                                } else {
                                    Collections.sort(preSortGroup, dataSortComparator);
                                    limitOffsetAssist = true;
                                }
                            } else {
                                if (limitOffsetSettingPattern != -1) {
                                    if (totalSortTargetCnt > limitOffsetIndexs[0]) {
                                        Collections.sort(preSortGroup, dataSortComparator);
                                    }
                                } else { 
                                    Collections.sort(preSortGroup, dataSortComparator);
                                }
                            }

                            //SystemLog.println(preSortGroup.size() + "=" +  dataSortComparator);
                            resultList.addAll(preSortGroup);
                        }

                        Collections.sort(tailList, dataSortComparator);
                        resultList.addAll(tailList);

                        Collections.sort(nullDataList, dataSortComparator);
                        //SystemLog.println(dataSortComparator);
                        resultList.addAll(nullDataList);
                        //SystemLog.println("resultList.size()=" +resultList.size());
                    } else {
                        // desc
                        // 振り分けが大きいものをまず投入
                        Collections.sort(tailList, dataSortComparator);
                        resultList.addAll(tailList);

                        // PreSortを逆順に回す
                        for (Iterator ite = preSortMap.descendingMap().entrySet().iterator(); ite.hasNext();) {
                            Map.Entry entry = (Map.Entry)ite.next();

                            List preSortGroup = (List)entry.getValue();
                            totalSortTargetCnt = totalSortTargetCnt + preSortGroup.size();

                            if (limitOffsetSettingPattern != -1 && totalSortTargetCnt > limitOffsetIndexs[0]) {
                                if (limitOffsetAssist == true && totalSortTargetCnt > (limitOffsetIndexs[0] + limitOffsetIndexs[1])) {
                                    limitOffsetAssist = true;
                                } else {
                                    Collections.sort(preSortGroup, dataSortComparator);
                                    limitOffsetAssist = true;
                                }
                            } else {
                                if (limitOffsetSettingPattern != -1) {
                                    if (totalSortTargetCnt > limitOffsetIndexs[0]) {
                                        Collections.sort(preSortGroup, dataSortComparator);
                                    }
                                } else { 
                                    Collections.sort(preSortGroup, dataSortComparator);
                                }
                            }

                            //SystemLog.println(preSortGroup.size() + "=" +  dataSortComparator);
                            resultList.addAll(preSortGroup);
                        }

                        // NULL用を入れる
                        Collections.sort(nullDataList, dataSortComparator);
                        resultList.addAll(nullDataList);
                    }
                    //long end3 = System.nanoTime();
                    //SystemLog.println("time3=" + (end3 - start3));

                } else {
                    Collections.sort(resultList, dataSortComparator);
                }

                
                //SystemLog.println(dataSortComparator);
                return limitOffset(resultList, selectParameter);
            } else {
                if (resultList.size() > 0) {
                    return limitOffset(resultList, selectParameter);
                } else {
                    return resultList;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            // orderby エラー
            return new ArrayList();
        }
    }


    /**
     * Limit Offsetは簡易実装.<br>
     * TODO:order by と合わせて今後修正<br>
     */
    private List<TableDataTransfer> limitOffset(List<TableDataTransfer> resultList, SelectParameter selectParameter) {
        try {
            int limitOffsetSettingPattern = limitOffsetSettingPattern(selectParameter);
            if(resultList != null && (limitOffsetSettingPattern == 1 || limitOffsetSettingPattern == 2 || limitOffsetSettingPattern == 3)) {
            
                // limit offset実行
                int[] fromToIdx = createLimitOffsetPosition(limitOffsetSettingPattern, resultList.size(), selectParameter);
                return resultList.subList(fromToIdx[0], fromToIdx[1]);
            } else {
                return resultList;
            }
        } catch (IllegalArgumentException e) {
            // limit offset 指定間違い
            return new ArrayList();
        }
    }

    // Limit offset の設定パターンを返す
    // 1=両方
    // 2=limit が指定
    // 3=offset が指定
    private int limitOffsetSettingPattern(SelectParameter selectParameter) {
        int ret = -1;
        if(selectParameter.getLimit() != -1 && selectParameter.getOffset() != -1) {
            // limit offsetが全部指定
            ret = 1;
        } else if(selectParameter.getLimit() != -1 && selectParameter.getOffset() == -1) {
            // limit が指定
            ret = 2;
        } else if(selectParameter.getLimit() == -1 && selectParameter.getOffset() != -1) {
            // offset が指定
            ret = 3;
        }
        return ret;
    }

    // Limit offset からList上のstart位置、end位置を導きだす
    private int[] createLimitOffsetPosition(int limitOffsetPattern, int listSize, SelectParameter selectParameter) {
        int[] ret = new int[2];
        if(limitOffsetPattern == 1) {

            // limit offsetが全部指定
            int fromIndex = selectParameter.getOffset() - 1;
            if (fromIndex < 0) fromIndex = 0;
            int toIndex = fromIndex + selectParameter.getLimit();

            if (toIndex > listSize) toIndex = listSize;
            ret[0] = fromIndex;
            ret[1] = toIndex;
        } else if(limitOffsetPattern == 2) {

            // limit が指定
            int fromIndex = 0;
            int toIndex = fromIndex + selectParameter.getLimit();

            if (toIndex > listSize) toIndex = listSize;
            ret[0] = fromIndex;
            ret[1] = toIndex;
        } else if(limitOffsetPattern == 3) {

            // offset が指定
            int fromIndex = selectParameter.getOffset() - 1;
            if (fromIndex < 0) fromIndex = 0;
            int toIndex = listSize;
            ret[0] = fromIndex;
            ret[1] = toIndex;
       }
        return ret;
    }
}