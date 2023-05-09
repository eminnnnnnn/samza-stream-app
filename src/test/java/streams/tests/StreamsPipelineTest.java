package streams.tests;

import java.time.Duration;
import java.util.List;

import org.apache.samza.operators.KV;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;

import streams.SamzaPipeline;
import streams.data.User;
import streams.data.UniqueStats;
import streams.tests.utils.TestDataGenerator;

public class StreamsPipelineTest {
    @Test
    public void testTumblingWindowExample() {
        List<KV<String,User>> users = TestDataGenerator.genUsers();

//        users.forEach(u -> System.out.println(u.value)); // awesome println debugging
        InMemorySystemDescriptor inMemorySystem =
                new InMemorySystemDescriptor("test-kafka");

        InMemoryInputDescriptor<KV<String, User>> userInputDescriptor =
                inMemorySystem.getInputDescriptor(
                        SamzaPipeline.INPUT_STREAM_ID,
                        new NoOpSerde<KV<String, User>>()
                );

        InMemoryOutputDescriptor<Long> uniqueUsersOutputDescriptor =
                inMemorySystem.getOutputDescriptor(
                        SamzaPipeline.USERS_OUTPUT_STREAM_ID,
                        new NoOpSerde<Long>()
                );
        InMemoryOutputDescriptor<Long> uniqueCountriesOutputDescriptor =
                inMemorySystem.getOutputDescriptor(
                        SamzaPipeline.COUNTRIES_OUTPUT_STREAM_ID,
                        new NoOpSerde<Long>()
                );

        TestRunner
                .of(new SamzaPipeline())
                .addInputStream(userInputDescriptor, users)
                .addOutputStream(uniqueUsersOutputDescriptor, 1)
                .addOutputStream(uniqueCountriesOutputDescriptor, 1)
                .run(Duration.ofMinutes(1));

        TestRunner
                .<Long>consumeStream(uniqueUsersOutputDescriptor,
                                     Duration.ofMillis(1000))
                .get(0)
                .forEach(uniqStat -> {
                    System.out.println(uniqStat);
                    Assert.assertTrue(uniqStat > 0);
                });

        TestRunner
                .<Long>consumeStream(uniqueCountriesOutputDescriptor,
                                     Duration.ofMillis(1000))
                .get(0)
                .forEach(uniqStat -> {
                    System.out.println(uniqStat);
                    Assert.assertTrue(uniqStat > 0);
                });
    }

}
