package femtodb.core.accessor.parameter;

import java.util.*;


/** 
 * SelectParameterクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class SelectParameter {

    private String tableName = null;
    private List<NormalWhereParameter> normalWhereParameterList = null;
    private List<NormalWhereParameter> normalWhereParameterListLog = null;

    private List<SortParameter> sortParameterList = null;

    private int limit = -1;
    private int offset = -1;

    private NormalWhereParameter indexWhereParameter = null;

    public SelectParameter() {
    }

    public void setNormalWhereParameterListObject(List<NormalWhereParameter> normalWhereParameterList) {
        this.normalWhereParameterList = normalWhereParameterList;
        this.normalWhereParameterListLog = new ArrayList();
        if (normalWhereParameterList != null) {
            for (int i = 0; i < this.normalWhereParameterList.size(); i++) {
                this.normalWhereParameterListLog.add(this.normalWhereParameterList.get(i));
            }
        }
    }

    public void setIndexWhereParameterObject(NormalWhereParameter indexWhereParameter) {
        this.indexWhereParameter = indexWhereParameter;
    }

    public List<NormalWhereParameter> getNormalWhereParameterListObject() {
        return normalWhereParameterList;
    }

    public NormalWhereParameter getIndexWhereParameterObject() {
        return indexWhereParameter;
    }
    
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        return this.tableName;
    }


    public boolean existIndexWhereParameter() {
        if (indexWhereParameter == null) return false;
        return true;
    }

    public boolean existNormalWhereParameter() {
        if (normalWhereParameterList == null) return false;
        return true;
    }

    public boolean existSortParameter() {
        if (sortParameterList == null) return false;
        return true;
    }

    /**
     * 通常のWhere条件を追加
     *
     */
    public void addNormalWhereParameter(String columnName, int whereType, IWhereParameter parameter) {
        if (this.normalWhereParameterList == null) {
            this.normalWhereParameterList = new ArrayList();
            this.normalWhereParameterListLog = new ArrayList();
        }
        NormalWhereParameter normalWhere = new NormalWhereParameter(columnName, whereType, parameter);
        this.normalWhereParameterList.add(normalWhere);
        this.normalWhereParameterListLog.add(normalWhere);
    }

    // Indexは1つのカラムのみセット可能
    public void setIndexWhereParameter(String columnName, int whereType, IWhereParameter parameter) {

        NormalWhereParameter indexWhere = new NormalWhereParameter(columnName, whereType, parameter);
        this.indexWhereParameter = indexWhere;

        if (normalWhereParameterList == null) {
            normalWhereParameterList = new ArrayList();
            this.normalWhereParameterListLog = new ArrayList();
        }
        NormalWhereParameter normalWhere = new NormalWhereParameter(columnName, whereType, parameter);
        this.normalWhereParameterList.add(normalWhere);
    }

    public NormalWhereParameter getIndexWhereParameter() {
        return this.indexWhereParameter;
    }

    public int normalWhereParameterSize() {
        return normalWhereParameterList.size();
    }

    // 条件が終了した場合nullを返す
    public NormalWhereParameter nextNormalWhereParameter() {
        if (normalWhereParameterList != null && normalWhereParameterList.size() > 0) {
            return normalWhereParameterList.remove(0);
        } else {
            return null;
        }
    }

    public int sortParameterSize() {
        return sortParameterList.size();
    }

    // 条件が終了した場合nullを返す
    public List<SortParameter> getSortParameterList() {
        return sortParameterList;
    }

    /**
     * Sort条件を追加
     * sortType:1=asc, desc
     * numberSort=true　数値ソート
     */
    public void addSortParameter(String columnName, int sortType, boolean numberSort) {
        if (this.sortParameterList == null) {
            this.sortParameterList = new ArrayList();
        }

        SortParameter sortParameter = new SortParameter(columnName, sortType, numberSort);
        this.sortParameterList.add(sortParameter);
    }

    public String getTableString() {
        return tableName;
    }

    public String getNormalWhereParameterListString() {
        if (normalWhereParameterListLog == null) return null;
        StringBuilder strBuf = new StringBuilder();
        String sep ="";
        for (int i = 0; i < normalWhereParameterListLog.size(); i++) {
            strBuf.append(sep);
            strBuf.append(normalWhereParameterListLog.get(i).toString());
            sep = "\n";
        }
        return strBuf.toString();
    }

    public void setNormalWhereParameterListString(String str) {
        if (str == null && str.trim().equals("")) return;

        String[] parameterStrList = str.split("\n");
        for (int i = 0; i < parameterStrList.length; i++) {
            NormalWhereParameter normalWhereParameter = new NormalWhereParameter(parameterStrList[i]);

            addNormalWhereParameter(normalWhereParameter.getColumnName(), normalWhereParameter.getWhereType(), normalWhereParameter.getParameter());
        }
    }

    public String getIndexWhereParameterString() {
        StringBuilder strBuf = new StringBuilder();
        String sep ="";
        if (indexWhereParameter != null) {
            strBuf.append(indexWhereParameter.toString());
        }
        return strBuf.toString();
    }


    public void setIndexWhereParameterString(String str) {
        if (str == null && str.trim().equals("")) return;

        NormalWhereParameter normalWhereParameter = new NormalWhereParameter(str);
        setIndexWhereParameter(normalWhereParameter.getColumnName(), normalWhereParameter.getWhereType(), normalWhereParameter.getParameter());
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }


    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return this.limit;
    }


    public int getOffset() {
        return this.offset;
    }
}