import io.radicalbit.nsdb.api.java.InsertResult;
import io.radicalbit.nsdb.api.java.NSDB;

public class NSDBWrite {

    public static void main(String[] args) throws Exception {
        NSDB nsdb = NSDB.connect("127.0.0.1", 7817).get();

        NSDB.Bit bit = nsdb.db("root")
                .namespace("registry")
                .bit("people")
                .value(new java.math.BigDecimal("13"))
                .dimension("city", "Mouseton")
                .dimension("gender", "M")
                .dimension("double", 12.5)
                .dimension("bigDecimalLong", new java.math.BigDecimal("12"))
                .dimension("bigDecimalDouble", new java.math.BigDecimal("12.5"));

        InsertResult result = nsdb.write(bit).get();
        System.out.println("IsSuccessful = " + result.isCompletedSuccessfully());
        System.out.println("errors = " + result.getErrors());
    }
}
