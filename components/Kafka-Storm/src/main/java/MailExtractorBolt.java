package storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class MailExtractorBolt implements IRichBolt{
	private OutputCollector collector;

	
	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector collector) {
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		String event = input.getString(0);
		event = event.replace("\"", "");
		int index = event.indexOf("mails_group");
		if(index > 0) {
			String mails = event.substring(index);
			mails = mails.substring(mails.indexOf("[")+1, mails.indexOf("]"));
			String[] mailsArray = mails.split(",");
			for(String mail : mailsArray)
			collector.emit(new Values(mail));
		}
		collector.ack(input);
	}

	
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("mail"));
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
