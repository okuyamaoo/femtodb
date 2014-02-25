package femtodb.core;

/** 
 * TableInfoExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class TableInfoException extends DBException {

    public TableInfoException(Throwable t) {
        super(t);
    }

    public TableInfoException(String msg) {
        super(msg);
    }

    public TableInfoException(String msg, Throwable t) {
        super(msg, t);
    }
}