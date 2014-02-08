package femtodb.core;


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