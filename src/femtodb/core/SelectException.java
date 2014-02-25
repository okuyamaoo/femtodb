package femtodb.core;

/** 
 * SelectExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class SelectException extends DBException {

    public SelectException(Throwable t) {
        super(t);
    }

    public SelectException(String msg) {
        super(msg);
    }

    public SelectException(String msg, Throwable t) {
        super(msg, t);
    }
}