package com.datastax.sampledata;

import java.io.IOException;
import java.util.List;

import com.datastax.bulkloader.BulkLoadTransactions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSSTables {

    private static Logger logger = LoggerFactory.getLogger(CreateSSTables.class);

    public static void createSSTables(BulkLoadTransactions bulkLoader, int totalTrans) throws IOException {
				
		int batch = 100;
		int cycles = totalTrans / batch;	
				
		for (int i=0; i < cycles; i++){
			List<Transaction> transactions = TransactionGenerator.generatorTransaction(batch);
			bulkLoader.loadTransactions(transactions);
			
			if (cycles % batch == 0){
				logger.info("Wrote {} of {} cycles. Batch size: {}", i, cycles, batch);
			}
		}				
		bulkLoader.finish();
		
		logger.info("Finished file with {} transactions.", totalTrans);
	}

}
