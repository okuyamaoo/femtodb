package femtodb.core;

/** 
 * UpdateExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class UpdateException extends DBException {

    public UpdateException(Throwable t) {
        super(t);
    }

    public UpdateException(String msg) {
        super(msg);
    }

    public UpdateException(String msg, Throwable t) {
        super(msg, t);
    }
}