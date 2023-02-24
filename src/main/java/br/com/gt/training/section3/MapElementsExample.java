package br.com.gt.training.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.nio.channels.Pipe;

public class MapElementsExample {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline
                .apply(TextIO.read().from("src/main/resources/files/customer.csv"));

        PCollection<String> poutput = pCustList.apply(
                MapElements.into(TypeDescriptors.strings())
                        .via((String obj) -> obj.toUpperCase())
        );

        poutput.apply(TextIO.write().to("src/main/resources/files/cust_out.csv")
                .withNumShards(1)
                .withSuffix(".csv")
        );

        pipeline.run();
    }
}
