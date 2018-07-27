import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.mail.MessagingException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;



public class SparkConsumer {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkConsumerKafka");
		//.setMaster("local[*]");
		//JavaSparkContext sc = new JavaSparkContext(conf);
		//JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(2000));
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));


		Set<String> topics = Collections.singleton("github-stream-output");
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092,anotherhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class);
		kafkaParams.put("value.deserializer", StringDeserializer.class);
		kafkaParams.put("group.id", "spark-streaming-group");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(jssc,	LocationStrategies.PreferConsistent(), ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

		stream.foreachRDD(rdd -> {
			System.out.println("--- New RDD with " + rdd.partitions().size()
					+ " partitions and " + rdd.count() + " records");
			rdd.foreach(record -> {
				System.out.println(">>>>>>>>>>>>RECORD:" + record.value());
				String event = record.value();
				event = event.replace("\"", "");
				int index = event.indexOf("mails_group");
				if(index > 0) {
					String mails = event.substring(index);
					mails = mails.substring(mails.indexOf("[")+1, mails.indexOf("]"));
					String[] mailsArray = mails.split(",");
					MailSender sender = new MailSender();
					String mittente = "emanueletusoni@gmail.com";
					String oggetto = "Notifica evento";
					String messaggio = "Un nuovo evento si è verificato su uno degli account a cui collabora";
					for(String mail : mailsArray) {
						System.out.println(">>>>>>>>>>>>EMAIL:" + mail);
						try {
							sender.sendMail(mittente, mail, oggetto, messaggio);
						} catch (MessagingException e) {
							System.out.println("Errore nell'invio del messaggio tramite e-mail:" + e.toString());
							e.printStackTrace();
						}
					}
				}

				});
		});

		jssc.start();
		jssc.awaitTermination();

	}
}
