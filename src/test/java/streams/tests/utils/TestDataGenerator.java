package streams.tests.utils;

import java.util.ArrayList;
import java.util.Date;
import java.time.Instant;
import java.util.List;

import net.datafaker.Faker;
import org.apache.samza.operators.KV;

import streams.data.User;

public class TestDataGenerator {
    public static List<KV<String, User>> genUsers() {
        Faker faker = new Faker();
        List<KV<String, User>> users = new ArrayList<>();
        int MAX_USERS = 1000;
        for (int i = 0; i < MAX_USERS; i++) {
            User user = User.builder()
                    .name(faker.name().username())
                    .country(faker.country().name())
                    .date(Date.from(Instant.now()))
                    .build();
            users.add(KV.of(user.getName(), user));
        }
        return users;
    }
}
