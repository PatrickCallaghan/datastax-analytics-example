package com.datastax.dao;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.sampledata.Transaction;

public class TransactionsDao {

	private Session session;
	private static AtomicLong TOTAL_PRODUCTS = new AtomicLong(0);

	private static String keyspaceName = "datastax_analytics_demo";
	private static String tableNameProduct = keyspaceName + ".transactions";

	private static final String SELECT_ALL_PRODUCTS = "Select * from " + tableNameProduct;

	private PreparedStatement selectStmtProduct;

	public TransactionsDao(Session session) {
		this.session = session;
		
		this.selectStmtProduct = session.prepare(SELECT_ALL_PRODUCTS);
		this.selectStmtProduct.setConsistencyLevel(ConsistencyLevel.QUORUM);
	}

	public void getAllProducts(BlockingQueue<Transaction> processorQueue){

		Statement stmt = new SimpleStatement("Select * from " + tableNameProduct);
		stmt.setFetchSize(2500);
		ResultSet resultSet = this.session.execute(stmt);

		Iterator<Row> iterator = resultSet.iterator();

		while (iterator.hasNext()) {

			Row row = iterator.next();

			Transaction transaction = createTransactionFromRow(row);

			processorQueue.offer(transaction);			
			TOTAL_PRODUCTS.incrementAndGet();
		}
	}

	private Transaction createTransactionFromRow(Row row) {
		Transaction trans = new Transaction();
		trans.setAcountId(row.getString("accid"));
		trans.setAmount(row.getDouble("amount"));
		trans.setTxtnId(row.getUUID("txtnid").toString());
		trans.setTxtnDate(row.getDate("txtntime"));
		trans.setReason(row.getString("reason"));
		trans.setType(row.getString("type"));

		return trans;
	}

	public long getTotalProducts() {
		return TOTAL_PRODUCTS.longValue();
	}
}
