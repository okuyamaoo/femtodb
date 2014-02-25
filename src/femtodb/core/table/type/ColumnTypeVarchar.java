package femtodb.core.table.type;


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