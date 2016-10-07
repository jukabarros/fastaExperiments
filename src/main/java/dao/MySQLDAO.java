package dao;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import config.ConnectMySQL;
import config.ReadProperties;
import file.OutputFasta;

public class MySQLDAO{

	private String query;

	private Connection conn;

	private PreparedStatement queryExec;
	// variavel que insere a cada 500 mil registros
	private int insertAll;

	public MySQLDAO() throws IOException {
		this.query = null;
		this.conn = null;
		this.queryExec = null;
		this.insertAll = 0;
	}

	/*
	 * Metodo before e after sao usados para
	 * abrir e fechar conexao
	 */
	public void beforeExecuteQuery() throws SQLException{
		Properties prop;
		try {
			prop = ReadProperties.getProp();
			String database = prop.getProperty("database.name"); 
			this.conn = new ConnectMySQL().connectMysql(database);
			this.conn.setAutoCommit(false);
			this.insertAll = 0;
		} catch (IOException e) {
			System.out.println("Erro na execução da query: "+e.getMessage());
		}
	}

	public void afterExecuteQuery() throws SQLException{
		if (this.insertAll > 0) {
			this.queryExec.executeBatch();
		}
		this.conn.commit();
		this.queryExec.close();
		this.conn.close();
		this.insertAll = 0;
	}


	public void insertFastaInfo(String fileName, long size, String comment) throws SQLException{
		try{
			beforeExecuteQuery();

			this.query = "INSERT INTO fasta_info (file_name, size, comment) VALUES (?,?,?);";
			this.queryExec = this.conn.prepareStatement(query);
			this.queryExec.setString(1, fileName);
			this.queryExec.setLong(2, size);
			this.queryExec.setString(3, comment);
			this.queryExec.execute();

			afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}
	/**
	 * prepara a query para inserir muitos dados
	 * @throws SQLException
	 */
	public void prepareInsertFastaCollect() throws SQLException{
		this.query = "INSERT INTO fasta_collect (id_seq, seq_dna, line, fasta_info) VALUES (?,?,?,?);";
		this.queryExec = this.conn.prepareStatement(query);
	}

	public void insertFastaCollect(String idSeq, String seqDNA, int line, int fastaInfo) throws SQLException{
		try{
			this.queryExec.setString(1, idSeq);
			this.queryExec.setString(2, seqDNA);
			this.queryExec.setInt(3, line);
			this.queryExec.setInt(4, fastaInfo);
			this.queryExec.addBatch();
			this.insertAll++;
			if (this.insertAll%1000==0){
				this.queryExec.executeBatch();
				this.conn.commit();
				this.queryExec.clearBatch();
			}

		}catch (Exception e){
			System.out.println("Erro ao inserir o registro: :( \n"+e.getMessage());
		}
	}

	/**
	 * Atualiza o numero de linhas existente em um arquivo
	 * apos a sua leitura.
	 * @param fileName
	 * @param numOfLines
	 * @throws SQLException
	 */
	public void updateNumOfLinesFastaInfo(String fileName, int numOfLines) throws SQLException{
		try{
			this.beforeExecuteQuery();
			this.query = "UPDATE fasta_info SET num_line = ? WHERE file_name = ?;";
			this.queryExec = this.conn.prepareStatement(this.query);
			this.queryExec.setInt(1, numOfLines);
			this.queryExec.setString(2, fileName);
			this.queryExec.execute();
			this.afterExecuteQuery();
		}catch (Exception e){
			System.out.println("Erro ao atualizar o numero de linhas :( \n"+e.getMessage());
		}
	}
	
	/**
	 * Recupera o ID do arquivo fasta consultado pelo nome
	 * @param fileName nome do arquivo
	 * @return id
	 * @throws SQLException
	 */
	public int getIDFastaInfo(String fileName) throws SQLException{
		beforeExecuteQuery();

		this.query = "SELECT * FROM fasta_info WHERE file_name = ?";
		this.queryExec = this.conn.prepareStatement(query);
		this.queryExec.setString(1, fileName);
		ResultSet results = this.queryExec.executeQuery();
		int id = 0;
		while (results.next()){
			id = results.getInt(1);
		}

		afterExecuteQuery();

		return id;

	}

	/**
	 * Metodo retorna o nome do arquivo que foi consultado por ID
	 * @param idFastaInfo
	 * @return
	 * @throws SQLException
	 */
	private String getFileNameFastaInfo(int idFastaInfo) throws SQLException{
		this.query = "SELECT * FROM fasta_info WHERE id = ?";
		this.queryExec = this.conn.prepareStatement(query);
		this.queryExec.setInt(1, idFastaInfo);
		ResultSet results = this.queryExec.executeQuery();
		String fileName = null;
		while (results.next()){
			fileName = results.getString(2);
		}

		return fileName;

	}

	/**
	 * Metodo retorna o nome do arquivo que foi consultado por ID
	 * @param idFastaInfo
	 * @return
	 * @throws SQLException
	 */
	private Integer getNumOfLinesFastaInfo(int idFastaInfo) throws SQLException{
		this.query = "SELECT * FROM fasta_info WHERE id = ?";
		this.queryExec = this.conn.prepareStatement(this.query);
		this.queryExec.setInt(1, idFastaInfo);
		ResultSet results = this.queryExec.executeQuery();
		int numOfLine = 0;
		while (results.next()){
			numOfLine = results.getInt(4);
		}

		return numOfLine;

	}
	
	/**
	 * Metodo retorna todos os arquivos armazenandos e indexados na tabela fasta_info
	 * @return lista com o nome de todos os arquivos
	 * @throws SQLException
	 */
	public List<String> getAllFastaFile() throws SQLException{
		this.beforeExecuteQuery();
		this.query = "SELECT * FROM fasta_info";
		this.queryExec = this.conn.prepareStatement(this.query);
		ResultSet results = this.queryExec.executeQuery();
		List<String> allFastaFiles = new ArrayList<String>();
		while (results.next()){
			String fastaFile = results.getString("file_name");
			allFastaFiles.add(fastaFile);
		}
		this.afterExecuteQuery();
		return allFastaFiles;

	}

	/**
	 * Retorna o conteudo de um arquivo especifico
	 * @param fileName
	 * @return
	 * @throws SQLException
	 * @throws IOException 
	 */
	public void findByFilename(String fileName, int repeat, int srsSize) throws SQLException, IOException{
		// Recuperando o id do arquivo
		int fileID = this.getIDFastaInfo(fileName);
		OutputFasta outputFasta = new OutputFasta();
		beforeExecuteQuery();
		// recupera o numero total de linhas para ver se eh necessario fazer a extracao por partes
		Integer numOfLine = this.getNumOfLinesFastaInfo(fileID);
		System.out.println("* Criando o arquivo: "+fileName);
		outputFasta.createFastaFile(repeat+fileName);
		if (numOfLine <= 500000){
			this.query = "SELECT TRIM(id_seq), TRIM(seq_dna) FROM fasta_collect WHERE fasta_info = ?;";
			this.queryExec = this.conn.prepareStatement(this.query);
			this.queryExec.setInt(1, fileID);
			ResultSet results = this.queryExec.executeQuery();
			while (results.next()){
				outputFasta.writeFastaFile(results.getString(1), results.getString(2), srsSize);
			}
		}else{
			int numOfRecords = 0;
			int numParts = numOfLine/500000;
			for (int i = 0; i < numParts; i++) {
				if (i != (numParts - 1)){
					numOfLine = 500000;
				}
				this.query = "SELECT TRIM(id_seq), TRIM(seq_dna) FROM "
						+ "fasta_collect WHERE fasta_info = ? AND line LIMIT "+numOfRecords+","+numOfLine+";";
				numOfRecords += 500000;
				this.queryExec = this.conn.prepareStatement(this.query);
				this.queryExec.setInt(1, fileID);
				ResultSet results = this.queryExec.executeQuery();
				while (results.next()){
					outputFasta.writeFastaFile(results.getString(1), results.getString(2), srsSize);
				}
				System.out.println("* Registros escritos: "+numOfRecords+"/"+numOfLine);

				results.close();
				this.query = null;
			}
		}
		if (numOfLine.equals(0)){
			System.out.println("* Conteúdo do arquivo não encontrado no Banco de dados :(");
		}

		outputFasta.closeFastaFile();
		afterExecuteQuery();

		System.out.println();
		System.out.println("**** Quantidade de registros: "+numOfLine);

	}
	
	/**
	 * Consulta pelo identificador de sequência
	 * @param idSeqDNA
	 * @throws SQLException
	 */
	public void findByID(String idSeqDNA) throws SQLException{
		beforeExecuteQuery();	
		this.query = "SELECT * FROM fasta_collect WHERE id_seq = ?";
		this.queryExec = this.conn.prepareStatement(query);
		this.queryExec.setString(1, idSeqDNA);
		ResultSet results = this.queryExec.executeQuery();
		boolean idSeqFound = false;
		while (results.next()){
			String fileName = this.getFileNameFastaInfo(results.getInt(5));
			System.out.println("ID de Sequência encontrado no arquivo "+fileName);
			System.out.println("ID: "+results.getString(2));
			System.out.println("Sequência: "+results.getString(3));
			System.out.println("Linha: "+ results.getInt(4));
			idSeqFound = true;
		}
		if (!idSeqFound){
			System.out.println("*** ID "+idSeqDNA+" não encontrado no Banco de dados :(");
		}
		afterExecuteQuery();

	}

	public Connection getConn() {
		return conn;
	}

	public void setConn(Connection conn) {
		this.conn = conn;
	}

}
