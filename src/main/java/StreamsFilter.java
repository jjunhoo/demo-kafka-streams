import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsFilter {

    private final static String APPLICATION_NAME = "streams-filter-application";
    private final static String BOOTSTRAP_SERVER = "localhost:9092";
    private final static String STREAM_LOG = "stream_log";
    private final static String STREAM_LOG_FILTER = "stream_log_filter";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // 소스 프로세서
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamLog = builder.stream(STREAM_LOG); // STREAM_LOG 로부터 토픽의 데이터 추출

        // 스트림 프로세싱 및 싱크 프로세서
        streamLog.filter((key, value) -> value.length() > 5).to(STREAM_LOG_FILTER); // STREAM_LOG 로부터 추출한 데이터 프로세싱 및 싱크 프로세서 저장

        // 실행
        KafkaStreams streams;
        streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }
}
