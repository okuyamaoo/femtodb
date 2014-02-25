package femtodb.core;


/** 
 * DuplicateDeleteExceptionクラス<br>
 *
 * @author Takahiro Iwase
 * @license Apache License 2.0 
 */
public class DuplicateDeleteException extends DBException {

    public DuplicateDeleteException(Throwable t) {
        super(t);
    }

    public DuplicateDeleteException(String msg) {
        super(msg);
    }

    public DuplicateDeleteException(String msg, Throwable t) {
        super(msg, t);
    }
}