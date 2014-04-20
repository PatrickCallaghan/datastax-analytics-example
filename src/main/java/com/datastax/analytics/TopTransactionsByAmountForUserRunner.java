package com.datastax.analytics;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.dao.TransactionsDao;
import com.datastax.demo.utils.PropertyHelper;
import com.datastax.demo.utils.Timer;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.sampledata.Transaction;

public class TopTransactionsByAmountForUserRunner{

	private static Logger logger = LoggerFactory.getLogger(TopTransactionsByAmountForUserRunner.class);	
	
	private BlockingQueue<Transaction> queue = new ArrayBlockingQueue<Transaction>(1000);
	private Session session;
	
	public TopTransactionsByAmountForUserRunner() {
		String contactPointsStr = PropertyHelper.getProperty("contactPoints", "localhost");
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
		
		TopTransactionsByAmountForUserProcessor topForUser = new TopTransactionsByAmountForUserProcessor(queue);
		executor.execute(topForUser);
		
		Cluster cluster = Cluster.builder().addContactPoints(contactPointsStr.split(",")).build();		
		this.session = cluster.connect();
		
		TransactionsDao dao = new TransactionsDao(session);
		
		Timer timer = new Timer();
		timer.start();
		
		dao.getAllProducts(queue);		
		Map<String, Transaction> results = topForUser.getResults();
		
		for (Transaction trans : results.values()){
			logger.info(trans.getAcountId() + " - " + trans.toString());
		}
		
		timer.end();
		logger.info("Analytics Runner took : " + timer.getTimeTakenMillis() + "ms");
		
		session.close();
		cluster.close();
		
		System.exit(0);
	}
	
	public static void main(String[] args) {
		new TopTransactionsByAmountForUserRunner();		
	}
}
