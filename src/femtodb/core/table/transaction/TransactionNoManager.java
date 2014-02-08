package femtodb.core.table.transaction;import java.util.*;import java.util.concurrent.*;import java.util.concurrent.atomic.AtomicLong;public class TransactionNoManager {    private static AtomicLong no = null;    private static Map<Long, Boolean> transactionStatusMap = null;    public static void initTransactionNoManager() {        no = new AtomicLong(1);        transactionStatusMap = new ConcurrentHashMap(1024);    }    public static void initTransactionNoManager(long transactionNo) {        no = new AtomicLong(transactionNo);        transactionStatusMap = new ConcurrentHashMap(1024);    }    public static TransactionNo createTransactionNo() {        long ret = no.incrementAndGet();        transactionStatusMap.put(ret, false);        return new TransactionNo(ret);    }    public static boolean commitTransaction(TransactionNo no) {        no.commit();        transactionStatusMap.remove(no.getTransactionNo());        return true;    }    public static boolean rollbackTransaction(TransactionNo no) {        no.rollback();        transactionStatusMap.remove(no.getTransactionNo());        return true;    }}