package femtodb.core.accessor.executor;import java.util.*;import femtodb.core.*;import femtodb.core.table.*;import femtodb.core.table.data.*;import femtodb.core.table.transaction.*;import femtodb.core.accessor.parameter.*;import femtodb.core.accessor.executor.*;import femtodb.core.accessor.scripts.*;public class NormalWhereExecutor {    private NormalWhereParameter normalWhereParameter = null;    private TableInfo tableInfo = null;    private IWhereType whereType = null;    private String targetColumnName = null;    public NormalWhereExecutor(NormalWhereParameter normalWhereParameter, TableInfo tableInfo) {        this.normalWhereParameter = normalWhereParameter;        this.tableInfo = tableInfo;        compile();    }    private void compile() {        if (normalWhereParameter.getWhereType() == IWhereType.WHERE_TYPE_EQUAL) {            equalCompile();        } else if (normalWhereParameter.getWhereType() == IWhereType.WHERE_TYPE_LIKE) {            likeCompile();        } else if (normalWhereParameter.getWhereType() == IWhereType.WHERE_TYPE_LARGE) {            largeCompile();        } else if (normalWhereParameter.getWhereType() == IWhereType.WHERE_TYPE_SMALL) {            smallCompile();        }    }    private void equalCompile() {        this.targetColumnName = normalWhereParameter.getColumnName();        this.whereType = new WhereTypeEqual(this.targetColumnName, normalWhereParameter.getParameter());    }    private void likeCompile() {        this.targetColumnName = normalWhereParameter.getColumnName();        this.whereType = new WhereTypeLike(this.targetColumnName, normalWhereParameter.getParameter());    }    private void largeCompile() {        this.targetColumnName = normalWhereParameter.getColumnName();        this.whereType = new WhereTypeLarge(this.targetColumnName, normalWhereParameter.getParameter());    }    private void smallCompile() {        this.targetColumnName = normalWhereParameter.getColumnName();        this.whereType = new WhereTypeSmall(this.targetColumnName, normalWhereParameter.getParameter());    }    public final void execute(TableIterator iterator, TransactionNo transactionNo, List resultList) {                whereType.executeAll(iterator, transactionNo, resultList);    }    public final boolean execute(TableDataTransfer tableDataTransfer) {        return whereType.execute(tableDataTransfer);    }}