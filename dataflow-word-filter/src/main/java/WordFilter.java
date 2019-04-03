import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.util.Arrays;

public class WordFilter {

    public static void main(String[] args) {

        CustomOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation().as(CustomOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(TextIO.read().from(options.getBucket() + "/**"))
                .apply(ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void apply(ProcessContext c) {
                        String[] splitList = c.element().split(",");
                        Arrays.stream(splitList).forEach(c::output);
                    }
                }))
                .apply(Distinct.create())
                .apply(BigQueryIO.<String>write()
                    .to(options.getTableRef())
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                    .withFormatFunction(value -> {
                        TableRow tableRow = new TableRow();
                        tableRow.set("value", value);
                        return tableRow;
                    })
                );

        pipeline.run();

    }

    private interface CustomOptions extends DataflowPipelineOptions {

        String getBucket();
        void setBucket(String bucket);

        String getTableRef();
        void setTableRef(String tableRef);

    }

}
