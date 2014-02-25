package femtodb.core;

/** 
 * DuplicateUpdateExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DuplicateUpdateException extends DBException {

    public DuplicateUpdateException(Throwable t) {
        super(t);
    }

    public DuplicateUpdateException(String msg) {
        super(msg);
    }

    public DuplicateUpdateException(String msg, Throwable t) {
        super(msg, t);
    }
}