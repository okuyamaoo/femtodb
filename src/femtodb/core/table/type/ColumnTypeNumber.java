package femtodb.core.table.type;


public class ColumnTypeNumber extends AbstractColumnType implements IColumnType {

    /**
     * カラムの型を返す.<br>
     *
     * @return カラムの型番号
     */
    public int getType() {
        return 2;
    }

    public String toString(){
        return "Number type";
    }
}