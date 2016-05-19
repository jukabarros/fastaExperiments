package create;

import java.text.DecimalFormat;
import java.text.NumberFormat;

import com.datastax.driver.core.Session;

import config.ConnectCassandra;


/*
 * Cria um keyspace com uma tabela que possui uma unica
 * coluna que vai ser inserida o ID com a sequencia
 */
public class CreateExperiment1 {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	
	public CreateExperiment1() {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
	}
	
	public void createKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating keyspace fastaExperiment1...");
		try{
			this.query = "CREATE KEYSPACE fastaExperiment1 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void createTables(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Creating tables...");
		try{
			this.query = "CREATE TABLE fastaExperiment1.fastaCollect (all_data text PRIMARY KEY)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void dropKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("Dropping Keyspace fastaExperiment1");
		try{
			this.query = "DROP KEYSPACE fastaExperiment1";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void truncateTable(String table){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			this.query = "TRUNCATE "+table;
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		CreateExperiment1 create = new CreateExperiment1();
		create.dropKeyspace();
		System.out.println("OK");
		create.createKeyspace();
		System.out.println("OK");
		create.createTables();
		System.out.println("OK");
		long endTime = System.currentTimeMillis();
		
		long totalTime = endTime - startTime;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("******** EXECUTION TIME: " + formatter.format(totalTime / 1000d) + " seconds");

	}

}
