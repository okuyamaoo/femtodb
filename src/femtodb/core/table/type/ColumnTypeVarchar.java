package femtodb.core.table.type;


/** 
 * ColumnTypeVarcharクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class ColumnTypeVarchar extends AbstractColumnType implements IColumnType {

    /**
     * .<br>
     *
     * @return 
     */
    public int getType() {
        return 1;
    }

    public String getTypeString() {
        return "equal";
    }

    public String toString(){
        return "Varchar type";
    }

}