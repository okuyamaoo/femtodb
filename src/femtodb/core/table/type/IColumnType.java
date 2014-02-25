package femtodb.core.table.type;

public interface IColumnType {

    public int VARCHAR_COLUMN = 1;
    public int NUMBER_COLUMN = 2;
    public int TEXT_COLUMN = 3;

    public int getType();

    // タイプの表記文字列を返す
    public String getTypeString();

    public String toString();
}