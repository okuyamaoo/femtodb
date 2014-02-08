package femtodb.core.table.type;

abstract class AbstractColumnType {

    /**
     * カラムの型を返す.<br>
     *
     * @return カラムの型番号
     */
    abstract int getType(); 
}