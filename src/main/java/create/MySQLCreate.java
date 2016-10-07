package create;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import config.ConnectMySQL;
import config.ReadProperties;

public class MySQLCreate {

	private String query;
	private String database;

	public MySQLCreate() throws IOException {
		this.query = null;
		Properties prop = ReadProperties.getProp();
		this.database = prop.getProperty("database.name"); 
	}

	public void createDatabase() throws SQLException{
		try{
			// Conexao vai vazio pois o banco ainda nao foi criado
			Connection conectar = new ConnectMySQL().connectMysql("");
			System.out.println("Criando Banco de Dados "+this.database);
			this.query = "CREATE DATABASE IF NOT EXISTS "+this.database+";";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
		}catch(SQLException e){
			System.out.println("*** Erro ao criar o banco de dados: \n"+e.getMessage());
		}
	}

	public void dropDatabase() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql("");
			System.out.println("Limpando o Banco de Dados "+this.database);
			this.query = "DROP DATABASE IF EXISTS "+this.database+";";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
		}catch(SQLException e){
			System.out.println("*** Erro ao deletar o banco de dados: \n "+e.getMessage());
		}
	}

	/*
	 * Cria 2 tabelas: fasta_info e fasta_collect
	 * com relacao de 1 para N
	 */
	public void createTables() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql(this.database);
			System.out.println("Criando a tabela: fasta_info");
			this.query = "CREATE TABLE IF NOT EXISTS fasta_info (id INT NOT NULL AUTO_INCREMENT,"
					+ " file_name VARCHAR(52), size DOUBLE(50,3), num_line BIGINT(20), "
					+ " comment VARCHAR (200), PRIMARY KEY (id));";
			PreparedStatement queryExec0 = conectar.prepareStatement(this.query);
			queryExec0.execute();
			queryExec0.close();

			System.out.println("Criando a tabela: fasta_collect");
			this.query = "CREATE TABLE IF NOT EXISTS fasta_collect (id INT PRIMARY KEY AUTO_INCREMENT,"
					+ " id_seq TEXT, seq_dna TEXT, line INT(50), fasta_info INT, "
					+ " FOREIGN KEY fasta_collect (fasta_info)"
					+ " REFERENCES fasta_info (id) ON DELETE CASCADE);";

			PreparedStatement queryExec1 = conectar.prepareStatement(this.query);
			queryExec1.execute();
			queryExec1.close();
		}catch(SQLException e){
			System.out.println("*** Erro ao criar a tabela: \n "+e.getMessage());
		}
	}

	public void truncateTables() throws SQLException{
		try{
			Connection conectar = new ConnectMySQL().connectMysql(this.database);
			System.out.println("Limpando a tabela fasta_collect");
			this.query = "TRUNCATE fasta_collect;";
			PreparedStatement queryExec = conectar.prepareStatement(this.query);
			queryExec.execute();
			queryExec.close();
			// Truncate nao pode por causa da chave estrangeira
			System.out.println("Limpando a tabela fasta_info");
			this.query = "DELETE FROM fasta_info;";
			PreparedStatement queryExec0 = conectar.prepareStatement(this.query);
			queryExec0.execute();
			queryExec0.close();


		}catch(SQLException e){
			System.out.println("*** Erro ao limpar a tabela: \n "+e.getMessage());
		}
	}

	// Prepara o ambiente do MySQL
	public static void main(String[] args) throws SQLException, IOException {
		MySQLCreate mc = new MySQLCreate();
		System.out.println("**** CRIANDO AMBIENTE DO MYSQL ****");
		mc.dropDatabase();
		mc.createDatabase();
		mc.createTables();
		mc.truncateTables();
		
		System.out.println("\nOK");
	}

}
