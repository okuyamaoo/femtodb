package femtodb.core.table.type;


public class ColumnTypeText extends AbstractColumnType implements IColumnType {

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