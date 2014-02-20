package femtodb.core.accessor;

import java.util.*;

import femtodb.core.*;
import femtodb.core.table.*;
import femtodb.core.table.data.*;
import femtodb.core.table.transaction.*;
import femtodb.core.accessor.parameter.*;
import femtodb.core.accessor.executor.*;
import femtodb.core.accessor.scripts.*;


public class SelectTableAccessor {


    TableManager tableManager = null;

    public SelectTableAccessor(TableManager tableManager) {
        this.tableManager = tableManager;
    }

    public ResultStruct select(SelectParameter selectParameter, TransactionNo transactionNo) {
        List resultList = null;
        long start = System.nanoTime();
        if (selectParameter.existNormalWhereParameter() || selectParameter.existIndexWhereParameter()) {

            resultList = select(selectParameter.getTableName(), selectParameter, transactionNo);
        } else {
            // 全件取得
            resultList = select(selectParameter.getTableName(), transactionNo);
        }

        long end = System.nanoTime();
        System.out.println("select - section1 time=" + (end - start));
        // limit offset 前の件数
        int baseResultCount = resultList.size();

        start = System.nanoTime();
        // order by は簡易実装
        if (selectParameter.existSortParameter()) {
            resultList = orderBy(resultList, selectParameter);
            //resultList = limitOffset(resultList, selectParameter);
        } else {
            // Limit Offset は簡易実装
            resultList = limitOffset(resultList, selectParameter);
        }
        end = System.nanoTime();
        System.out.println("select - section2 time=" + (end - start));

        // 結果のフォルダー
        ResultStruct resultStruct = new ResultStruct(baseResultCount, resultList);

        return resultStruct;
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
        for (; iterator.hasNext();) {

            iterator.nextEntry();
            TableData tableData = (TableData)iterator.getEntryValue();
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
            if (tableDataTransfer != null) {
                allData.add(tableDataTransfer);
            }
        }
long end = System.nanoTime();
System.out.println("select - all time=" + (end - start));
        return allData;
    }


    /**
     * 条件適応
     *
     *
     */
    protected List<TableDataTransfer> select(String tableName, SelectParameter selectParameter, TransactionNo transactionNo) {
        ITable table = this.tableManager.getTableData(tableName);
        List<TableDataTransfer> allData = new ArrayList<TableDataTransfer>(table.getRecodeSize());

        // 条件に対してオプティマイザがIndex条件などを加味し適応済みのIteratorを返す
        TableIterator iterator = QueryOptimizer.execute(transactionNo, tableManager, tableName, selectParameter, table);

        NormalWhereParameter normalWhereParameter = selectParameter.nextNormalWhereParameter();
        NormalWhereExecutor normalWhereExecutor = null;
        if (normalWhereParameter != null) {
            normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
        }

        for (; iterator.hasNext();) {

            iterator.nextEntry();
            TableData tableData = (TableData)iterator.getEntryValue();
            TableDataTransfer tableDataTransfer = tableData.getTableDataTransfer(transactionNo);
            if (tableDataTransfer != null) {
                if (normalWhereParameter != null) {
                    if (normalWhereExecutor.execute(tableDataTransfer)) {
                        allData.add(tableDataTransfer);
                    }
                } else {
                    allData.add(tableDataTransfer);
                }
            }
        }

        while ((normalWhereParameter = selectParameter.nextNormalWhereParameter()) != null) {
            normalWhereExecutor = null;
            normalWhereExecutor = new NormalWhereExecutor(normalWhereParameter, tableManager.getTableInfo(tableName));
            if (allData.size() > 0) {
                int size = allData.size();
                List<TableDataTransfer> tmpAllData = new ArrayList<TableDataTransfer>(size);

                for (TableDataTransfer targetTableDataTransfer:allData) {

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

                // TODO: Limit offsetを同時に実行する
                DataSortComparator dataSortComparator = new DataSortComparator(selectParameter.getSortParameterList());
                int resultListSize = resultList.size();

                if (resultListSize > 10000) {
                    int limitOffsetSettingPattern = limitOffsetSettingPattern(selectParameter);
                    int[] limitOffsetIndexs = createLimitOffsetPosition(limitOffsetSettingPattern, resultListSize, selectParameter);

                    List<SortParameter> list = selectParameter.getSortParameterList();
                    SortParameter firstSortParameter = list.get(0);
                    int preSortListSize = resultListSize / 400;
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

long end1 = System.nanoTime();
System.out.println("time1=" + (end1 - start1) + " preSortMapSIze=" + preSortMap.size());
long start2 = System.nanoTime();
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
long end2 = System.nanoTime();
System.out.println("time2=" + (end2 - start2));

long start3 = System.nanoTime();

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

                            //System.out.println(preSortGroup.size() + "=" +  dataSortComparator);
                            resultList.addAll(preSortGroup);
                        }

                        Collections.sort(tailList, dataSortComparator);
                        resultList.addAll(tailList);

                        Collections.sort(nullDataList, dataSortComparator);
                        //System.out.println(dataSortComparator);
                        resultList.addAll(nullDataList);
                        //System.out.println("resultList.size()=" +resultList.size());
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

                            //System.out.println(preSortGroup.size() + "=" +  dataSortComparator);
                            resultList.addAll(preSortGroup);
                        }

                        // NULL用を入れる
                        Collections.sort(nullDataList, dataSortComparator);
                        resultList.addAll(nullDataList);
                    }
long end3 = System.nanoTime();
System.out.println("time3=" + (end3 - start3));

                } else {
                    Collections.sort(resultList, dataSortComparator);
                }

                
                System.out.println(dataSortComparator);
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