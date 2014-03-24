package femtodb.core.table.type;

import java.io.*;

/** 
 * ColumnTypeVarcharクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class ColumnTypeVarchar extends AbstractColumnType implements IColumnType, Serializable {

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