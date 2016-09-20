package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import config.ConnectCassandra;
import config.ReadProperties;
import create.CassandraCreate;
import file.OutputFasta;

public class CassandraDAO {
	
	private ConnectCassandra connCassandra;
	private Session session;
	private String query;
	private String keyspace;
	private CassandraCreate cassandraCreate;
	private PreparedStatement statement;
	
	private BatchStatement batch;
	
	public CassandraDAO() throws IOException {
		super();
		this.connCassandra = new ConnectCassandra();
		this.session = null;
		this.query = null;
		Properties prop = ReadProperties.getProp();
		this.keyspace =  prop.getProperty("cassandra.keyspace");
		this.cassandraCreate = new CassandraCreate();
		this.statement = null;
		this.batch = new BatchStatement();
	}
	
	public void beforeExecuteQuery(){
		this.connCassandra.connect();
		this.session = this.connCassandra.getCluster().connect(this.keyspace);
		
	}
	
	public void afterExecuteQuery(){
		if (this.batch.size() > 0) {
			this.session.execute(batch);
		}
		this.connCassandra.close();
	}
	
	public void executeQuery(String query){
		try{
			this.session.execute(query);
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Insere as informacoes do arquivo fasta na tabela fasta_info, que vai servir
	 * como index para os arquivos que serão inseridos
	 * 
	 * @param fileName
	 * @param size
	 * @param comment
	 */
	public void insertFastaInfo(String fileName, double size, String comment){
		try{
			this.beforeExecuteQuery();
			this.query = "INSERT INTO fasta_info (file_name, size, comment, num_lines) VALUES (?, ?, ?, 0);";
			this.statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(fileName, size, comment));
			this.afterExecuteQuery();
			this.cassandraCreate.createTable(fileName);
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Prepara a query para inserir grandes massas de dados
	 * @param table
	 */
	public void prepareInsert(String table){
		try{
			String tableName = table.replace(".", "___");
			this.query = "INSERT INTO "+tableName+" (id_seq, seq_dna, line) VALUES (?, ?, ?);";
			this.statement = this.session.prepare(query);
			
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Insere o conteudo do arquivo fasta na tabela referente ao arquivo
	 * essa tabela possui o mesmo nome do arquivo
	 * @param id
	 * @param seqDna
	 * @param line
	 */
	public void insertData(String idSeq, String seqDna, int line){
		try{
			this.batch.add(this.statement.bind(idSeq, seqDna, line));
			if (this.batch.size() >= 50000){
				this.session.execute(this.batch);
				this.batch = new BatchStatement();
			}
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Insere as informacoes do arquivo fasta na tabela fasta_info, que vai servir
	 * como index para os arquivos que serão inseridos
	 * 
	 * @param fileName
	 * @param size
	 * @param comment
	 */
	public void updateNumOfLinesFastaInfo(String fileName, int numOfLines){
		try{
			this.beforeExecuteQuery();
			this.query = "UPDATE fasta_info SET num_lines = ? WHERE file_name = ?;";
			this.statement = this.session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			this.session.execute(boundStatement.bind(numOfLines, fileName));
			this.afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao executar a query: :("+e.getMessage());
		}
	}
	
	/**
	 * Recupera todos os arquivos fastas que foram inseridos e indexados na tabela fasta_info
	 * @return lista com o nome dos arquivos fasta
	 * @throws IOException
	 * 
	 */
	public List<String> getAllFastaFile() throws IOException{
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fasta_info;";
		this.statement = this.session.prepare(query);
		ResultSet results = this.session.execute(this.query);
		List<String> allFastaFiles = new ArrayList<String>();
		for (Row row : results) {
			if (row.isNull(0)){
				System.out.println("** Conteudo nao encontrado no banco de dados");
				break;
			}
			allFastaFiles.add(row.getString("file_name"));
		}
		this.afterExecuteQuery();
		
		return allFastaFiles;
	}
	
	/**
	 * Verifica se o arquivo consultado existe, atraves de uma consulta
	 * na tabela fasta_info. Se existir faz a extracao do conteudo no metodo
	 * extractFastaContent.
	 * Lembrando que o nome da tabela ao inves de finalizar com .fa ou .fasta
	 * está "___fa".
	 * @param fileName
	 * @return
	 * @throws IOException 
	 */
	public void findByFileName(String fileName, int numOfSample, int srsSize) throws IOException{
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fasta_info WHERE file_name = ?;";
		this.statement = this.session.prepare(query);
		BoundStatement boundStatement = new BoundStatement(statement);
		ResultSet results = this.session.execute(boundStatement.bind(fileName));
	
		for (Row row : results) {
			if (row.isNull(0)){
				System.out.println("** Conteudo nao encontrado no banco de dados");
				break;
			}
			String tableName = fileName.replace(".", "___");
			int numOfLines = row.getInt("num_lines");
			this.extractFastaContent(tableName, numOfLines, numOfSample, srsSize);
		}
		this.afterExecuteQuery();
	}
	
	/**
	 * Extrai o conteudo de um arquivo fasta
	 * @param table tabela que contem o conteudo do arquivo fasta
	 * @param numOfLines numero de linhas total
	 * @param repeat repeticao do experimento
	 * @param srsSize tamanho da SRS
	 * @throws IOException
	 */
	private void extractFastaContent(String table, int numOfLines, int numOfSample, int srsSize) throws IOException{
		OutputFasta outputFasta = new OutputFasta();
		String fileName = table.replace("___", ".");
		System.out.println("** Criando o arquivo "+fileName);
		outputFasta.createFastaFile(numOfSample+fileName);
		if (numOfLines == 0){
			System.out.println("*** Esse arquivo está vazio :(");
		}else {

			this.query = "SELECT * FROM "+table;
			ResultSet results = this.session.execute(this.query);
			int line = 0;
			for (Row row : results) {
				outputFasta.writeFastaFile(row.getString("id_seq"), row.getString("seq_dna"), srsSize);
				if (line%1000000 == 0){
					System.out.println("* Registros escritos: "+line+"/"+numOfLines);
				}
				line++;
			}
		}
		System.out.println("* Quantidade de linhas: "+numOfLines);
		outputFasta.closeFastaFile();
	}
	
	/**
	 * Metodo que consulta um ID de um sequencia especifca em todo o banco
	 * eh feita uma consulta para receber todos os arquivos existente no banco
	 * e feita a consulta em cada tabela.
	 * @param idSeq
	 */
	public void findByID(String idSeq){
		this.beforeExecuteQuery();
		String query0 = "SELECT * FROM fasta_info;";
		ResultSet results0 = this.session.execute(query0);
		for (Row row0 : results0) {
			String tableName = row0.getString("file_name").replace(".", "___");
			
			this.query = "SELECT * FROM "+tableName+" WHERE id_seq = ?;";
			this.statement = this.session.prepare(this.query);
			BoundStatement boundStatement = new BoundStatement(statement);
			ResultSet results = this.session.execute(boundStatement.bind(idSeq));
			for (Row row : results) {
				System.out.println("ID de Sequência encontrado no arquivo "+row0.getString("file_name"));
				System.out.println("ID de Sequência: "+row.getString("id_seq"));
				System.out.println("Sequência DNA: "+row.getString("seq_dna"));
				System.out.println("Linha: "+row.getInt("line"));
			}
		}
		this.afterExecuteQuery();
	}

}
