package femtodb.core.accessor.parameter;/**  * SmallWhereParameterクラス<br> * * @author Takahiro Iwase * @license Apache License 2.0  */public class SmallWhereParameter implements IWhereParameter {    private String parameter = null;    public SmallWhereParameter(String parameter) {        this.parameter = parameter;    }    public String toStoreString() {        return "SmallWhereParameter=" + this.parameter;    }    public String toString() {                return this.parameter;    }}