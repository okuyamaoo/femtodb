package femtodb.core.table.type;

/** 
 * AbstractColumnType<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
abstract class AbstractColumnType {

    /**
     * カラムの型を返す.<br>
     *
     * @return カラムの型番号
     */
    abstract int getType(); 
}