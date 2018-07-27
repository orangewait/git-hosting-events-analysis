package storm;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaStorm {

	public static void main(String[] args) throws Exception{	
		Config config = new Config();
		String zkConnString = "localhost:2181";
		String topic = "github-stream-output";
		ZkHosts zkhost = new ZkHosts(zkConnString);
		SpoutConfig kafkaSpoutConfig = new SpoutConfig (zkhost, topic, "/" + topic, UUID.randomUUID().toString());
		kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaSpoutConfig);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafka-spout", kafkaSpout);
		builder.setBolt("mail-extractor-bolt", new MailExtractorBolt()).shuffleGrouping("kafka-spout");
		builder.setBolt("mail-sender-bolt", new MailSenderBolt()).shuffleGrouping("mail-extractor-bolt");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("KafkaStorm", config, builder.createTopology());

		//Thread.sleep(10000);

		//cluster.shutdown();

	}

}
