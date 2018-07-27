import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;



public class KafkaConsumerCassandraWriter
{
	private final static String TOPIC = "github-stream-output";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	private static Consumer<Long, String> createConsumer()
	{
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumerTest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		// Create the consumer using props.
		final Consumer<Long, String> consumer =	new KafkaConsumer<>(props);

		// Subscribe to the topic.
		consumer.subscribe(Collections.singletonList(TOPIC));

		return consumer;
	}



	private static void runConsumer() throws InterruptedException
	{
		System.out.println("runConsumer()");
		final Consumer<Long, String> consumer = createConsumer();

		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
		Session session = cluster.connect("giteventskeyspace");

		final int giveUp = 100;
		int noRecordsCount = 0;

		while (true)
		{
			System.out.println("dentro while");

			final ConsumerRecords<Long, String> consumerRecords =
					consumer.poll(1000);

			if(consumerRecords.count() == 0)
			{
				noRecordsCount++;

				if(noRecordsCount > giveUp) break;
				else continue;
			}

			noRecordsCount = 0;

			PreparedStatement preparedInsertStatement =
				session.prepare(
					"INSERT INTO events JSON :json"
				);

			BoundStatement insertStatement =
					preparedInsertStatement
					.bind();

			consumerRecords.forEach(record ->
			{
				String json = record.value();

				System.out.println(json);

				insertStatement.setString("json", json);

				session.execute(insertStatement);

				System.out.println("Inserted");
			});

			consumer.commitAsync();
		}

		session.close();
		consumer.close();
		System.out.println("DONE");
	}



	public static void main(String... args) throws Exception
	{
		runConsumer();
	}
}
