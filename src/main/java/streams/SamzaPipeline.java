package streams;

import java.io.Serializable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;

import streams.data.User;
import streams.data.UniqueStats;

public class SamzaPipeline implements StreamApplication, Serializable {
    Logger logger = Logger.getLogger("samza-logs");
    private static final String KAFKA_SYSTEM_NAME = "kafka";
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT =
            ImmutableList.of("localhost:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS =
            ImmutableList.of("localhost:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS =
            ImmutableMap.of("replication.factor", "1");

    public static final String INPUT_STREAM_ID = "users-input";
    public static final String USERS_OUTPUT_STREAM_ID = "unique-users-output";
    public static final String COUNTRIES_OUTPUT_STREAM_ID = "unique-countries-output";
    private static final Duration WINDOW_INTERVAL = Duration.ofMillis(100);

    @Override
    public void describe(StreamApplicationDescriptor streamApplicationDescriptor) {
        KafkaSystemDescriptor kafkaSysDesc = new KafkaSystemDescriptor(KAFKA_SYSTEM_NAME)
            .withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
            .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
            .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        // Serdes
        KVSerde<String, User> userKVSerde =
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(User.class));
        KVSerde<String, Long> statKVSerde =
            KVSerde.of(new StringSerde(), new LongSerde());
        KVSerde<String, UniqueStats> uniqueStatsKVSerde =
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(UniqueStats.class));

        // Input Descriptors
        KafkaInputDescriptor<KV<String, User>> userInputDescriptor =
            kafkaSysDesc.getInputDescriptor(INPUT_STREAM_ID, userKVSerde);

        // Output Descriptors
        KafkaOutputDescriptor<Long> uniqueUsersOutDesc =
            kafkaSysDesc.getOutputDescriptor(USERS_OUTPUT_STREAM_ID, new LongSerde());
        KafkaOutputDescriptor<Long> uniqueCountriesOutDesc =
            kafkaSysDesc.getOutputDescriptor(COUNTRIES_OUTPUT_STREAM_ID, new LongSerde());

        streamApplicationDescriptor.withDefaultSystem(kafkaSysDesc);

        // Input Streams
        MessageStream<KV<String, User>> usersInStream =
            streamApplicationDescriptor.getInputStream(userInputDescriptor);

        // Output Streams
        OutputStream<Long> usersOutStream =
            streamApplicationDescriptor.getOutputStream(uniqueUsersOutDesc);
        OutputStream<Long> countriesOutStream =
                streamApplicationDescriptor.getOutputStream(uniqueCountriesOutDesc);

        // Processing
        MessageStream<WindowPane<Void, Collection<User>>> windowInStream =
            usersInStream
                .map(rec -> rec.value)
                .window(Windows.tumblingWindow(
                            WINDOW_INTERVAL,
                            new JsonSerdeV2<>(User.class)),
                        "window");

        MessageStream<Collection<User>> windowUserNames =
            windowInStream
                .map(WindowPane::getMessage);

        MessageStream<Collection<User>> windowCountries =
            windowInStream
                .map(WindowPane::getMessage);

        windowUserNames
            .map(users -> {
                Set<String> uniques = new HashSet<>();
                users.forEach(u -> uniques.add(u.getName()));
                return (long)uniques.size();
            })
            .sendTo(usersOutStream);

        windowCountries
            .map(users -> {
                Set<String> uniques = new HashSet<>();
                users.forEach(u -> uniques.add(u.getCountry().toLowerCase()));
                return (long)uniques.size();
            })
            .sendTo(countriesOutStream);
    }
}
