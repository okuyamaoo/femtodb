package femtodb.core;

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