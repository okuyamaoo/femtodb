package femtodb.core.table.type;


public class ColumnTypeVarchar extends AbstractColumnType implements IColumnType {

    /**
     * カラムの型を返す.<br>
     *
     * @return 
     */
    public int getType() {
        return 1;
    }

    public String toString(){
        return "Varchar type";
    }

}