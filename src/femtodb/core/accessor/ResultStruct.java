package femtodb.core.accessor;import java.util.List;import femtodb.core.table.data.*;/**  * ResultStructクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class ResultStruct {    // Limit, offsetをおこなう前の結果件数    private int baseResultCount = 0;        private List<TableDataTransfer> resultList = null;    public ResultStruct(int baseResultCount, List<TableDataTransfer> resultList) {        this.baseResultCount = baseResultCount;        this.resultList = resultList;    }    public int getBaseResultCount() {        return baseResultCount;    }    public List<TableDataTransfer> getResultList() {        return resultList;    }}