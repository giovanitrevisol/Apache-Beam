package br.com.gt.training.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

class User extends SimpleFunction<String, String>{
    @Override
    public String apply(String input) {
        String arr[] = input.split(",");
        String SId = arr[0];
        String UId = arr[1];
        String UName = arr[2];
        String VId = arr[3];
        String duration = arr[4];
        String startTime = arr[5];
        String sex = arr[6];

        String output = "";
        if(sex.equals("1")){
            output = SId + ", " + UId  + ", " + UName  + ", " + VId  + ", " + duration  + ", " + startTime  + "M" ;
        } else if (sex.equals("2")){
            output = SId + ", " + UId  + ", " + UName  + ", " + VId  + ", " + duration  + ", " + startTime  + "F" ;
        } else {
            output = input;
        }

        return output;
    }
}
public class MapElementsSimpleFunction {

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline
                .apply(TextIO.read().from("src/main/resources/files/user.csv"));

        //Using simple Function

        PCollection<String> pOutput = pCustList.apply(
                MapElements.via(new User())
        );

        pOutput.apply(TextIO.write().to("src/main/resources/files/user_out.csv")
                .withNumShards(1)
                .withSuffix(".csv")
        );

        pipeline.run();
    }

}
