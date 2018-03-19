import io.radicalbit.nsdb.api.java.NSDB;
import io.radicalbit.nsdb.api.java.QueryResult;

/**
 * This class is meant to be an example of a call to the execute Statement Apis.
 */
public class NSDBRead {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").query("select * from people limit 1");

        QueryResult result = nsdb.executeStatement(statement).get();

        if (result.isCompletedSuccessfully()) {
            System.out.println("db " + result.getDb());
            System.out.println("namespace " + result.getNamespace());
            System.out.println("metric " + result.getMetric());
            System.out.println("bits " + result.getRecords());
        } else {
            System.out.println("reason " + result.getReason());
        }
    }
}
