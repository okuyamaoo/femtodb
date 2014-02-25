package femtodb.core;


/** 
 * DeleteExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DeleteException extends DBException {

    public DeleteException(Throwable t) {
        super(t);
    }

    public DeleteException(String msg) {
        super(msg);
    }

    public DeleteException(String msg, Throwable t) {
        super(msg, t);
    }
}