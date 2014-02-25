package femtodb.core.accessor.parameter;import java.util.*;/**  * UpdateColumnParameterクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class UpdateColumnParameter {    private String columnName = null;    private IColumnUpdateParameter parameter = null;            public UpdateColumnParameter(String columnName, IColumnUpdateParameter parameter) {        this.columnName = columnName;        this.parameter = parameter;    }        public UpdateColumnParameter(String encodeString) {        String[] encodeStringSplit = encodeString.split("\t");        this.columnName = encodeStringSplit[0];        this.parameter = new StringUpdateParameter(encodeStringSplit[1]);    }    public String getColumnName() {        return this.columnName;    }    public String getColumnData() {        return this.parameter.toString();    }    public String toString() {        return this.columnName + "\t" + this.parameter.toString();    }}