import io.radicalbit.nsdb.api.java.NSDB;
import io.radicalbit.nsdb.rpc.response.RPCInsertResult;

public class NSDBWrite {

    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.Bit bit = nsdb.db("root")
            .namespace("registry")
            .bit("people")
            .value(new java.math.BigDecimal("13"))
            .dimension("city", "Mouseton")
            .dimension("gender", "M")
            .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
            .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"));

        RPCInsertResult result =  nsdb.write(bit).get();
        System.out.println(result);
    }
}
