package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.MySQLCreate;
import dao.MySQLDAO;
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

			String fileNameOutput = "";
			String idSeqDNA = "";

			String extractData = prop.getProperty("extract.data").toUpperCase();
			String searchbyID = prop.getProperty("search.byid").toUpperCase();
			
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
				if (searchbyID.equals("YES")){
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
				if (searchbyID.equals("YES")){
					System.out.println("****** CONSULTA MONGODB ******");
					frToMongodb.findSeqByID();
				}
			}else if (db.equals("MYSQL")){
				if(insertData.equals("YES")){
					for (int i = 1; i <= numOfSamples; i++) {
						MySQLCreate.main(null);
						System.out.println("***************** Repetição: "+i);
						FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
						frToMySQL.readFastaByDirectory(fastaDirectory, i, srsSize);
					}
				}else{
					MySQLDAO dao = new MySQLDAO();
					if (extractData.equals("YES")){
						System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
						dao.findByFilename(fileNameOutput, 0, srsSize);
					}else{
						System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
						dao.findByID(idSeqDNA);
					}
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
