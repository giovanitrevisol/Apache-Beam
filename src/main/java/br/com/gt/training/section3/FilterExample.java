package br.com.gt.training.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class MyFilter implements SerializableFunction<String, Boolean>{
    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}
public class FilterExample {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline
                .apply(TextIO.read().from("src/main/resources/files/customer_pardo.csv"));

        //using Filter

        PCollection<String> pOutput = pCustList.apply(Filter.by(new MyFilter()));

        pOutput.apply(TextIO.write().to("src/main/resources/files/customer_pardo_output.csv")
                .withHeader("ID, Name, Last Name, City")
                .withNumShards(1)
                .withSuffix(".csv")
        );

        pipeline.run();
    }
}
