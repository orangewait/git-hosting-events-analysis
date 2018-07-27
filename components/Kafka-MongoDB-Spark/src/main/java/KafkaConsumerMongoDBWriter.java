import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;



public class KafkaConsumerMongoDBWriter
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


	static void runConsumer() throws InterruptedException
	{
		System.out.println("runConsumer()");

		final Consumer<Long, String> consumer = createConsumer();
		MongoClient client = new MongoClient("localhost", 27017);
		MongoDatabase db = client.getDatabase("github_events");
		MongoCollection<Document> eventsCollection = db.getCollection("events");

		final int giveUp = 100;
		int noRecordsCount = 0;

		while (true)
		{
			final ConsumerRecords<Long, String> consumerRecords =
					consumer.poll(1000);

			if(consumerRecords.count() == 0)
			{
				noRecordsCount++;

				if(noRecordsCount > giveUp) break;
				else continue;
			}

			noRecordsCount = 0;

			consumerRecords.forEach(record ->
			{
				Document eventDocument = Document.parse(record.value());

				String timestamp = eventDocument.getString("timestamp");
				System.out.println("Il timestamp è : " + timestamp);

				Long timestampMillis = Instant.parse(timestamp).toEpochMilli();
				System.out.println("Il timestampMillis è : " + timestampMillis);

				Date data = new Date(timestampMillis);
				System.out.println("La data è : " + data);

				eventDocument.replace("timestamp", data);

				eventsCollection.insertOne(eventDocument);

				System.out.printf(
					"Consumer Record:(%d, %s, %d, %d)\n",
					record.key(),
					record.value(),
					record.partition(),
					record.offset()
					);
			});

			consumer.commitAsync();
		}

		client.close();
		consumer.close();
		System.out.println("DONE");
	}



	public static void main(String... args) throws Exception
	{
		runConsumer();
	}
}
