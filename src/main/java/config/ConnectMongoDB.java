package config;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Properties;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

public class ConnectMongoDB {

	private MongoClient mongo;
	private String database;
	private Properties prop;
	
	public ConnectMongoDB() throws IOException {
		super();
		this.mongo = null;
		this.prop = ReadProperties.getProp();
		this.database = prop.getProperty("mongodb.db");
	}
	
	/*
	 * Conecta com o Mongo
	 */
	public MongoDatabase connectToMongoDB() throws IOException{
		String host = this.prop.getProperty("mongodb.host");
		int port = Integer.parseInt(this.prop.getProperty("mongodb.port"));
		
		this.mongo = new MongoClient(host, port);
        
		MongoDatabase db = this.mongo.getDatabase(this.database);
		System.out.println(db.getName());
		
		return db; 
	}
	
	/*
	 * Deleta o banco de dados, usado somente durante a criacao.
	 */
	public void dropDatabase() throws UnknownHostException{
		String host = this.prop.getProperty("mongodb.host");
		int port = Integer.parseInt(this.prop.getProperty("mongodb.port"));
		
		this.mongo = new MongoClient(host, port);
		this.mongo.getDatabase(this.database);
		
		this.mongo.dropDatabase(this.database);
	}

}

