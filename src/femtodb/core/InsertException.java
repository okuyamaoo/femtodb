package femtodb.core;


/** 
 * InsertExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class InsertException extends DBException {
    public InsertException(Throwable t) {
        super(t);
    }

    public InsertException(String msg) {
        super(msg);
    }

    public InsertException(String msg, Throwable t) {
        super(msg, t);
    }
}