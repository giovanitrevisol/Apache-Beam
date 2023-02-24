public class RealTimeStreamingETL {

    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

        p.apply(KafkaIO.<Long, IotEvent>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("beamtopic")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(IotDeserializer.class)
                .withoutMetadata())
                .apply(Values.<IotEvent>create())
                .apply(Window.<IotEvent>into(FixedWindows.of(Duration.standardSeconds(10))))
                .apply(ParDo.of(new DoFn<IotEvent, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) {

                        if (c.element().getTemperature() > 80.0) {
                            c.output(c.element().getDeviceId());
                        }

                    }
                }))
                .apply(Count.perElement())
                .apply(JdbcIO.<KV<String, Long>>write().withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create("com.mysql.jdbc.Driver", "jdbc:mysql://127.0.0.1:3306/beamdb?useSSL=false")
                        .withUsername("root").withPassword("root"))
                        .withStatement("insert into event values (?,?) ")
                        .withPreparedStatementSetter(new PreparedStatementSetter<KV<String, Long>>() {

                            public void setParameters(KV<String, Long> element, PreparedStatement preparedStatement)
                                    throws Exception {
                                // TODO Auto-generated method stub
                                preparedStatement.setString(1, element.getKey());
                                preparedStatement.setLong(2, element.getValue());
                            }
                        }));

        p.run();

    }
}
