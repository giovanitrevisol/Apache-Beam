package br.com.gt.training;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {

    /*
    Para executar o projeto via terminal:
    1 - mavem install -> para gerar o .jar
    2 - acessar a pasta target via terminal - (giovanitrevisol@nb-005045:~/gio-dev/apache-beam/practical_apache_bean/section1/target$)
    3 - executar o seguinte comando:
     java -cp section1-1.0-SNAPSHOT-jar-with-dependencies.jar br.com.gt.training.LocalFileExample --inputFile="/home/giovanitrevisol/gio-dev/apache-beam/practical_apache_bean/section1/src/main/resources/files/input.csv" --outputFile="/home/giovanitrevisol/gio-dev/apache-beam/practical_apache_bean/section1/src/main/resources/files/output.csv" --extn=".csv"
     * */
    public static void main(String[] args) {

        MyOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(MyOptions.class);

        Pipeline pipeline = Pipeline.create();

        PCollection<String> output = pipeline
                .apply(TextIO.read()
                        .from(options.getInputFile()));

        output.apply(TextIO.write()
                .to(options.getOutputFile())
                .withNumShards(1)
                .withSuffix(options.getExtn()));

        pipeline.run();

    }


/*

--> COM OS DIRETORIOS EM HARDCODE

*     public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> output = pipeline
                .apply(TextIO.read()
                .from("src/main/resources/files/input.csv"));

        output.apply(TextIO.write()
                .to("src/main/resources/files/outputs.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();

    }
* */
}
