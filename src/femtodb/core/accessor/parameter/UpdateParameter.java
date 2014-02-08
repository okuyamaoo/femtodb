package femtodb.core.accessor.parameter;

import java.util.*;

public class UpdateParameter extends SelectParameter {

    private Map<String, UpdateColumnParameter> updateParameterMap = null;


    public UpdateParameter() {
    }


    public boolean existUpdateParameter() {
        if (updateParameterMap == null) return false;
        return true;
    }

    /**
     * Updateパラメータを追加
     *
     */
    public void setUpdateParameter(String columnName, IColumnUpdateParameter parameter) {
        if (updateParameterMap == null) updateParameterMap = new HashMap<String, UpdateColumnParameter>();
        UpdateColumnParameter updateColumnParameter = new UpdateColumnParameter(columnName, parameter);
        updateParameterMap.put(columnName, updateColumnParameter);
    }

    public List<UpdateColumnParameter> getUpdateParameterList() {
        if (updateParameterMap == null) return null;
        List<UpdateColumnParameter> retList = new ArrayList(updateParameterMap.size());

        for (Iterator it = updateParameterMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry)it.next();
            retList.add((UpdateColumnParameter)entry.getValue());
        }
        
        return retList;
    }

    public int updateParameterSize() {
        if (updateParameterMap == null) return 0;
        return updateParameterMap.size();
    }
}