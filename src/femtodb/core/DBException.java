package femtodb.core;

/** 
 * DBExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DBException extends Exception {
    public DBException(Throwable t) {
        super(t);
    }

    public DBException(String msg) {
        super(msg);
    }

    public DBException(String msg, Throwable t) {
        super(msg, t);
    }

}