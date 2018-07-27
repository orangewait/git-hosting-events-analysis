package storm;

import java.util.Map;
import java.util.Properties;

import javax.mail.MessagingException;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class MailSenderBolt implements IRichBolt{
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {
		String mail = input.getString(0);
		System.out.println("Input MailSenderBolt: "+ mail);
		MailSender sender = new MailSender();
		String mittente = "emanueletusoni@gmail.com";
		String oggetto = "Notifica evento";
		String messaggio = "Un nuovo evento si è verificato su uno degli account a cui collabora";
		try {
			sender.sendMail(mittente, mail, oggetto, messaggio);
		} catch (MessagingException e) {
			System.out.println("Errore nell'invio del messaggio tramite e-mail:" + e.toString());
			e.printStackTrace();
		}

	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
