package br.com.gt.training.section4;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline.apply(TextIO.read().from("src/main/resources/files/Distinct.csv"));

        PCollection<String> uniqCustomer = pCustList.apply(Distinct.<String>create());

        uniqCustomer.apply(TextIO.write().to("src/main/resources/files/Distinct_out.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }
}
