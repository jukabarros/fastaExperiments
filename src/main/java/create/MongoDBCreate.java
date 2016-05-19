package create;

import java.io.IOException;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoException;

import config.ConnectMongoDB;

public class MongoDBCreate {
	
	private ConnectMongoDB conMongoDB;
	private DB db;
	
	public MongoDBCreate() throws IOException {
		this.conMongoDB = new ConnectMongoDB();
		this.db = conMongoDB.connectToMongoDB();
	}
	
	/*
	 * Metodo pega  a collection desejada
	 * caso nao exista, ela cria uma nova
	 */
	public DBCollection getCollection(String collection) throws IOException{
		DBCollection coll = this.db.getCollection(collection);
		return coll;
	}
	
	/*
	 * Lista todas as coleções
	 */
	public Set<String> listAllColection() throws IOException{
		Set<String> allCollections = this.db.getCollectionNames();
		return allCollections;
	}
	
	/*
	 * Metodo deleta a collection
	 */
	public void dropCollection(String collection) throws IOException{
		System.out.println("Limpando collection "+collection);
		DBCollection coll = this.db.getCollection(collection);
		coll.drop();
	}
	
	public static void main(String[] args) throws IOException {
		try {
			System.out.println("**** CRIANDO AMBIENTE DO MONGODB ****");
			System.out.println("Excluindo o Banco de Dados");
			ConnectMongoDB mongoCon = new ConnectMongoDB();
			mongoCon.dropDatabase();
			System.out.println("OK");
			
			System.out.println("Criando o Banco de Dados");
			MongoDBCreate mongodbCreate = new MongoDBCreate();
			
			System.out.println("OK");
			Set<String> allCollections = mongodbCreate.listAllColection();
			String[] listAllCollections = allCollections.toArray(new String [allCollections.size()]);
			for (int i = 0; i < listAllCollections.length; i++) {
				String collection = listAllCollections[i];
				mongodbCreate.dropCollection(collection);
			}
			System.out.println("Criando a collection fasta_info");
			mongodbCreate.getCollection("fasta_info");
			System.out.println("OK");
			
		}  catch (MongoException e) {
			e.printStackTrace();
		}

	}
}
