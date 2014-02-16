package com.datastax.analytics;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import com.datastax.sampledata.Transaction;

public class TopTransactionsByAmountForUserProcessor implements Runnable{
	
	private BlockingQueue<Transaction> queue;
	private Map<String, Transaction> topTransactionsByUser = new HashMap<String, Transaction>();
	
	public TopTransactionsByAmountForUserProcessor(BlockingQueue<Transaction> queue) {
		this.queue = queue;
	}

	@Override
	public void run() {
		while(true){				
			Transaction transaction = queue.poll();
			
			if (transaction!=null){
				if (this.topTransactionsByUser.containsKey(transaction.getAcountId())){
					Transaction oldTransaction = this.topTransactionsByUser.get(transaction.getAcountId());
					
					if (oldTransaction==null){
						this.topTransactionsByUser.put(transaction.getAcountId(), transaction);
					}else{
						if (transaction.getAmount() > oldTransaction.getAmount()){
							this.topTransactionsByUser.put(transaction.getAcountId(), transaction);
						}
					}
				}else{
					this.topTransactionsByUser.put(transaction.getAcountId(), transaction);
				}
			}				
		}						
	}

	public Map<String, Transaction> getResults() {
		return topTransactionsByUser;
	}
}
