package com.datastax.sampledata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class CreateSampleData {

    private static Logger logger = LoggerFactory.getLogger(CreateSampleData.class);

	private BufferedWriter out;
	private int totalTrans = 50000000;

	public CreateSampleData() throws IOException {
		out = new BufferedWriter(new FileWriter("src/main/resources/Transactions.csv"));
		
		int batch = 1000;
		int cycles = this.totalTrans / batch;	
		
		for (int i=0; i < cycles; i++){
			List<Transaction> transactions = TransactionGenerator.generatorTransaction(batch);
			this.writeToFile(transactions);
			
			if (cycles % 1000 == 0){
				logger.info("Wrote {} of {} cycles", i, cycles);
			}
		}
		
		out.close();
		logger.info("Finished file with {} transactions.", totalTrans);
		System.exit(0);
	}

	public void writeToFile(List<Transaction> transactions) throws IOException {
		try {
			for (Transaction transaction : transactions) {
				out.write(transaction.toCVSString() + "\n");
			}
		} catch (IOException e) {
			throw e;
		}
	}

	public static void main(String[] args) {
		try {
			new CreateSampleData();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
