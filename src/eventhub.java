import org.json.JSONObject;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Hashtable;
import java.util.Random;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
/**
 * Created by Alexeev on 26-Jun-15.
 */
public class eventhub {
    public static void main(String[] args) throws NamingException,
            JMSException, IOException, InterruptedException, NamingException {

        Hashtable<String, String> env = new Hashtable<String, String>();
        env.put(Context.INITIAL_CONTEXT_FACTORY,
                "org.apache.qpid.amqp_1_0.jms.jndi.PropertiesFileInitialContextFactory");
        env.put(Context.PROVIDER_URL, "servicebus.properties");
        Context context = new InitialContext(env);

        ConnectionFactory cf = (ConnectionFactory) context.lookup("SBCF");

        Destination queue = (Destination) context.lookup("EventHub");


        Connection connection = cf.createConnection();


        final Session sendSession = connection.createSession(false,
                Session.AUTO_ACKNOWLEDGE);
        final MessageProducer sender = sendSession.createProducer(queue);


        Thread[] threads = new Thread[20];
        for(int i=0;i<threads.length;i++)
        {
            threads[i]=  new Thread(new Runnable() {
                @Override
                public void run() {
                    while (true) {
                        try {
                            sendBytesMessage(sendSession, sender);
                        } catch (JMSException e) {
                            e.printStackTrace();
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                        try {
                            Thread.sleep(20);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            threads[i].setDaemon(true);
            threads[i].setName("T("+i+")");
            threads[i].start();
        }


    }
    static Random r = new Random();
    public  static  String getRandomName()
    {
        String s = new String();
        int cnt = r.nextInt(5)+6;

        for(int i=0;i<cnt;i++)
        {
            int rr = r.nextInt('z'-'a')+'a';
            s+=(char)rr;
        }
        return  s;
    }
    private synchronized static void sendBytesMessage(Session sendSession, MessageProducer sender) throws JMSException, UnsupportedEncodingException, UnsupportedEncodingException {
        BytesMessage message = sendSession.createBytesMessage();
        String str = java.util.UUID.randomUUID().toString();

        JSONObject object = new JSONObject();
        object.put("Name",getRandomName());
        object.put("Age",r.nextInt(20)+18);
        object.put("GUID",str);
        str = object.toString();
        message.writeBytes(str.getBytes("UTF-8"));
        sender.send(message);
        System.out.println("Sent message : "+str);
    }
}
