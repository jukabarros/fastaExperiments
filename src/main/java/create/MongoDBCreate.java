package create;

import java.io.IOException;
import java.util.Iterator;

import org.bson.Document;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

import config.ConnectMongoDB;

public class MongoDBCreate {
	
	private ConnectMongoDB conMongoDB;
	private MongoDatabase db;
	
	public MongoDBCreate() throws IOException {
		this.conMongoDB = new ConnectMongoDB();
		this.db = conMongoDB.connectToMongoDB();
	}
	
	/*
	 * Metodo pega  a collection desejada
	 * caso nao exista, ela cria uma nova
	 */
	public MongoCollection<Document> getCollection(String collection) throws IOException{
		MongoCollection<Document> coll = this.db.getCollection(collection);
		return coll;
	}
	
	/*
	 * Metodo deleta a collection
	 */
	public void dropCollection(String collection) throws IOException{
		System.out.println("Limpando collection "+collection);
		MongoCollection<Document> coll = this.db.getCollection(collection);
		coll.drop();
	}
	
	public static void main(String[] args) throws IOException {
		try {
			MongoDBCreate mongodbCreate = new MongoDBCreate();
			System.out.println("**** CRIANDO AMBIENTE DO MONGODB ****");
			
			MongoIterable<String> allCollections = mongodbCreate.db.listCollectionNames();
			Iterator<String> it = allCollections.iterator();
			
			System.out.println("* Limpando as Coleções");
			while (it.hasNext()) {
				mongodbCreate.dropCollection(it.next());
			}

			System.out.println("* Criando a collection fasta_info");
			mongodbCreate.getCollection("fasta_info");
			
			System.out.println("OK");
			
		}  catch (MongoException e) {
			e.printStackTrace();
		}

	}
}
