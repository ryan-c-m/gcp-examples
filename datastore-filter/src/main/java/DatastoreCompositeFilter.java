import com.google.datastore.v1.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.HashMap;
import java.util.Map;

import static com.google.datastore.v1.client.DatastoreHelper.makeFilter;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

public class DatastoreCompositeFilter {

    private static final String PROJECT_ID = "gcp-batch-pattern";

    public static void main(String[] args) {
        run();
    }

    static void run() {

        Pipeline pipeline = Pipeline.create();

        Map<String, String> filterValues = new HashMap<>();
        filterValues.put("EXAMPLE_KEY", "EXAMPLE_VALUE");

        pipeline
                .apply(DatastoreIO.v1().read()
                    .withProjectId(PROJECT_ID)
                    .withQuery(filterQueryForKind("TokenData", filterValues)))
                .apply(ParDo.of(new DoFn<Entity, Entity>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        Entity e = c.element();
                        Map<String, Value> props = e.getPropertiesMap();
                        props.forEach((k,v) -> System.out.print(k));
                        c.output(e);
                    }
                }));

        pipeline.run().waitUntilFinish();
    }

    static Query filterQueryForKind(String kind, Map<String, String> filterValues) {
        Query.Builder queryBuilder = Query.newBuilder();
        queryBuilder.addKindBuilder().setName(kind);

        CompositeFilter.Builder compositeFilter = CompositeFilter.newBuilder();
        compositeFilter.setOp(CompositeFilter.Operator.AND);
        filterValues.forEach((k,v) -> compositeFilter.addFilters(makeFilter(k, PropertyFilter.Operator.EQUAL, makeValue(v))));

        queryBuilder.setFilter(Filter.newBuilder()
                .setCompositeFilter(compositeFilter).build());

        return queryBuilder.build();
    }

}
