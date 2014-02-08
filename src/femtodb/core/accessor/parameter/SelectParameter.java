package femtodb.core.accessor.parameter;

import java.util.*;

public class SelectParameter {

    private String tableName = null;
    private List<NormalWhereParameter> normalWhereParameterList = null;
    private NormalWhereParameter indexWhereParameter = null;

    public SelectParameter() {
    }

    public void setNormalWhereParameterListObject(List<NormalWhereParameter> normalWhereParameterList) {
        this.normalWhereParameterList = normalWhereParameterList;
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

    /**
     * 通常のWhere条件を追加
     *
     */
    public void addNormalWhereParameter(String columnName, int whereType, IWhereParameter parameter) {
        if (normalWhereParameterList == null) normalWhereParameterList = new ArrayList();
        NormalWhereParameter normalWhere = new NormalWhereParameter(columnName, whereType, parameter);
        this.normalWhereParameterList.add(normalWhere);
    }

    // Indexは1つのカラムのみセット可能
    public void setIndexWhereParameter(String columnName, int whereType, IWhereParameter parameter) {

        NormalWhereParameter indexWhere = new NormalWhereParameter(columnName, whereType, parameter);
        this.indexWhereParameter = indexWhere;

        if (normalWhereParameterList == null) normalWhereParameterList = new ArrayList();
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

}