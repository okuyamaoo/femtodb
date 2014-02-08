package femtodb.core;

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