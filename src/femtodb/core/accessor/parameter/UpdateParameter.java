package femtodb.core.accessor.parameter;

import java.util.*;

/** 
 * UpdateParameterクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
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


    public String getUpdateParameterMapString() {
        StringBuilder strBuf = new StringBuilder();
        String sep = "";
        for (Iterator it = updateParameterMap.entrySet().iterator(); it.hasNext();) {
            Map.Entry entry = (Map.Entry)it.next();
            strBuf.append(sep);
            UpdateColumnParameter param = (UpdateColumnParameter)entry.getValue();
            strBuf.append(param.toString());
            sep = "\n";
        }
        return strBuf.toString();
    }

    public void setUpdateParameterMapString(String mapStr) {
        String[] mapStrList = mapStr.split("\n");
        for (int i = 0; i < mapStrList.length; i++) {
            UpdateColumnParameter updateColumnParameter = new UpdateColumnParameter(mapStrList[i]);
            setUpdateParameter(updateColumnParameter.getColumnName(), new StringUpdateParameter(updateColumnParameter.getColumnData()));
        }
    }
}