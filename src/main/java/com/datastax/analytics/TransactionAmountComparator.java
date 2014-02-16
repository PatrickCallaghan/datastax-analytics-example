package com.datastax.analytics;

import java.util.Comparator;

import com.datastax.sampledata.Transaction;

public class TransactionAmountComparator implements Comparator<Transaction> {

	@Override
	public int compare(Transaction o1, Transaction o2) {
		if (o1.getAmount() > o2.getAmount()){
			return -1;
		}else if (o2.getAmount() > o1.getAmount()){
			return 1;
		}else{
			return 0;
		}			
	}
}
