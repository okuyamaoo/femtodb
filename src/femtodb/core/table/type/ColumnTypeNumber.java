package femtodb.core.table.type;


import java.io.*;

/** 
 * ColumnTypeNumberクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class ColumnTypeNumber extends AbstractColumnType implements IColumnType,Serializable {

    /**
     * カラムの型を返す.<br>
     *
     * @return カラムの型番号
     */
    public int getType() {
        return 2;
    }

    public String getTypeString() {
        return "number";
    }


    public String toString(){
        return "Number type";
    }
}