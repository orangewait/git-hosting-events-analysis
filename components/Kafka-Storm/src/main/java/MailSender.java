package storm;

import java.util.Properties;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Store;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class MailSender {
	private String mittente;
	private String destinatario;
	private String messaggio;

	public MailSender() {}
	
	public void sendMail(String mittente, String destinatario, String oggetto, String messaggio) throws MessagingException{
		// Creazione di una mail session
		//String username = "emanueletusoni@gmail.com";
		//String password = "D********!";
		//String host = "smtp.gmail.com";
		String host = "localhost";
		//String port = "587";
		Properties props = new Properties();
		props.put("mail.smtp.host", host);
		//props.setProperty("mail.smtp.port", port);
		//props.setProperty("mail.imap.ssl.enable", "true");
		//props.setProperty("mail.user", username);
		//props.setProperty("mail.password", password);
	    Session session = Session.getDefaultInstance(props);

	    
	 // Creazione del messaggio da inviare
	    MimeMessage message = new MimeMessage(session);
	    message.setSubject(oggetto);
	    message.setText(messaggio);
	    
	 // Aggiunta degli indirizzi del mittente e del destinatario
	    InternetAddress fromAddress = new InternetAddress(mittente);
	    InternetAddress toAddress = new InternetAddress(destinatario);
	    message.setFrom(fromAddress);
	    message.setRecipient(Message.RecipientType.TO, toAddress);
	    
	    Transport.send(message);
	    /*
	    Transport tr = session.getTransport("smtp");
	    tr.connect(host, username, password);
	    message.saveChanges();      // don't forget this
	    tr.sendMessage(message, message.getAllRecipients());
	    tr.close();
	    */

	}
}
