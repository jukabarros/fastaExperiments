package create;

import java.io.IOException;
import java.util.Properties;

import com.datastax.driver.core.Session;

import config.ConnectCassandra;
import config.ReadProperties;


/**
 * Cria um keyspace com uma tabela que possui uma unica
 * coluna que vai ser inserida o ID com a sequencia
 */
public class CassandraCreate {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	
	private String keyspace;
	private String replicationFactor;
	
	
	public CassandraCreate() throws IOException {
		super();
		Properties prop = ReadProperties.getProp();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
		this.keyspace =  prop.getProperty("database.name");
		this.replicationFactor = prop.getProperty("cassandra.replication.factor");
		
	}
	
	public void createKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			System.out.println("* Criando o keyspace "+this.keyspace);
			System.out.println("* Fator de Replica: "+this.replicationFactor);
			this.query = "CREATE KEYSPACE IF NOT EXISTS "+this.keyspace+" WITH replication = {'class':'SimpleStrategy',"
					+ " 'replication_factor':"+this.replicationFactor+"};";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	/*
	 * Tabela que recebera todas informacoes do arquivo fasta,
	 * sera usada para apontar a tabela que contem todo conteudo do arquivo
	 */
	public void createTableFastaInfo(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("* Criando a tabela fasta_info");
		try{
			this.query = "CREATE TABLE IF NOT EXISTS "+this.keyspace+".fasta_info"
					+ " (file_name text PRIMARY KEY, size double, comment text, num_lines int)";
			this.session.execute(this.query);
		}catch (Exception e) {
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	/**
	 * Tabela que recebe o mesmo nome do arquivo onde troca o .fa para -_fa
	 * pois o cassandra nao permite tabela com o "."
	 */
	public void createTable(String table){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			String tableName = table.replace(".", "___");
			this.query = "CREATE TABLE IF NOT EXISTS "+this.keyspace+"."+tableName+""
					+ "(id_seq text PRIMARY KEY, seq_dna text, line int)";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao criar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void dropKeyspace(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		System.out.println("* Deletando o Keyspace "+this.keyspace);
		try{
			this.query = "DROP KEYSPACE IF EXISTS "+this.keyspace+"";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao deletar o keyspace: "+e.getMessage());
		}
		this.connCassandra.close();
	}
	
	public void truncateTableFastaInfo(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect();
		try{
			System.out.println("* Limpando a tabela fasta_info");
			this.query = "TRUNCATE "+this.keyspace+".fasta_info";
			this.session.execute(this.query);
		}catch (Exception e){
			System.out.println("Erro ao limpar a tabela: "+e.getMessage());
		}
		this.connCassandra.close();
	}

	public static void main(String[] args) throws IOException {
		CassandraCreate create = new CassandraCreate();
		System.out.println("**** CRIANDO AMBIENTE DO CASSANDRA ****");
		create.dropKeyspace();
		create.createKeyspace();
		create.createTableFastaInfo();
		create.truncateTableFastaInfo();
		
		System.out.println("OK");

	}

}
