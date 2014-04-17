package femtodb.core.table.type;

import java.io.*;
/** 
 * ColumnTypeTextクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class ColumnTypeText extends AbstractColumnType implements IColumnType, Serializable {

    /**
     * Textサーチ用.<br>
     *
     * @return 
     */
    public int getType() {
        return 3;
    }

    public String getTypeString() {
        return "text";
    }

    public String toString(){
        return "Text type";
    }

}