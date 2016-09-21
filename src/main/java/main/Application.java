package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;

public class Application {

	/*
	 * Argumentos Opcionais:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Tamanho da SRS
	 */
	public static void main(String[] args) throws IOException, SQLException, InterruptedException {
		Application app = new Application();
		Properties prop = ReadProperties.getProp();
		int numOfSamples = Integer.parseInt(prop.getProperty("num.sample"));
		int numOfArgs = args.length;
		if (numOfArgs != 2) {
			System.out.println("*** Número de parametros inválidos: "+numOfArgs+" (2)");
		} else {
			
			String fastaDirectory = args[0];
			int srsSize = Integer.parseInt(args[1]);

			String db = prop.getProperty("database").toUpperCase();
			System.out.println("* Banco de Dados escolhido: "+db);
			System.out.println("* Número de Amostras: "+numOfSamples);
			System.out.println("* Tamanho da SRS: "+srsSize);

			String insertData = prop.getProperty("insert.data").toUpperCase();
			String extractData = prop.getProperty("extract.data").toUpperCase();
			String findByID = prop.getProperty("find.byid").toUpperCase();
			
			long startTime = System.currentTimeMillis();

			if(db.equals("CASSANDRA")){
				FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
				if(insertData.equals("YES")){
					System.out.println("****** INSERÇÃO CASSANDRA ******");
					frToCassandra.readFastaDirectory(fastaDirectory, numOfSamples, srsSize);
				}
				if (extractData.equals("YES")){
					System.out.println("****** EXTRAÇÃO CASSANDRA ******");
					frToCassandra.extractData(numOfSamples, srsSize);
				}
				if (findByID.equals("YES")){
					System.out.println("****** CONSULTA CASSANDRA ******");
					frToCassandra.findSeqByID();
				}
			}else if (db.equals("MONGODB")){
				FastaReaderToMongoDB frToMongodb = new FastaReaderToMongoDB();
				if(insertData.equals("YES")){
					System.out.println("****** INSERÇÃO MONGODB ******");
					frToMongodb.readFastaDirectory(fastaDirectory, numOfSamples, srsSize);
				}
				if (extractData.equals("YES")){
					System.out.println("****** EXTRAÇÃO MONGODB ******");
					frToMongodb.extractData(numOfSamples, srsSize);
				}
				if (findByID.equals("YES")){
					System.out.println("****** CONSULTA MONGODB ******");
					frToMongodb.findSeqByID();
				}
			}else if (db.equals("MYSQL")){
				FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
				if(insertData.equals("YES")){
					System.out.println("****** INSERÇÃO MYSQL ******");
					frToMySQL.readFastaDirectory(fastaDirectory, numOfSamples, srsSize);
				}
				if (extractData.equals("YES")){
					System.out.println("****** EXTRAÇÃO MYSQL ******");
					frToMySQL.extractData(numOfSamples, srsSize);
				}
				if (findByID.equals("YES")){
					System.out.println("****** CONSULTA MYSQL ******");
					frToMySQL.findSeqByID();
				}

			}  else{
				System.out.println("Opção de banco inválida :(");
			}

			long endTime = System.currentTimeMillis();
			app.calcTimeExecution(startTime, endTime);
		}


	}

	public long calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n*** Tempo TOTAL de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos ******\n");

		return totalTime;
	}

}
