import io.radicalbit.nsdb.api.java.NSDB;
import io.radicalbit.nsdb.api.java.QueryResponse;

public class NSDBRead {
    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.SQLStatement statement = nsdb.db("root").namespace("registry").query("select * from people limit 1");

        QueryResponse result = nsdb.executeStatement(statement).get();

        System.out.println(result);

    }
}
