package femtodb.core;

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