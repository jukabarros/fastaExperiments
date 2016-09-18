package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraDAO;
import dao.MongoDBDAO;
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
		String fileNameOutput = prop.getProperty("file.name.output");
		int numOfSamples = Integer.parseInt(prop.getProperty("num.sample"));
		int numOfArgs = args.length;
		if (numOfArgs != 2) {
			System.out.println("*** Número de parametros inválidos: "+numOfArgs+" (2)");
		} else {
			String fastaDirectory = args[0];
			int srsSize = Integer.parseInt(args[1]);

			String db = prop.getProperty("database").toUpperCase();
			System.out.println("* Banco de Dados escolhido: "+prop.getProperty("database").toUpperCase());
			System.out.println("* Número de Amostras: "+numOfSamples);
			System.out.println("* Tamanho da SRS: "+srsSize);

			String insertData = prop.getProperty("insert.data").toUpperCase();

			String idSeqDNA = prop.getProperty("id.seqDNA");
			String extractData = prop.getProperty("extract.data").toUpperCase();
			long startTime = System.currentTimeMillis();

			if(db.equals("CASSANDRA")){
				if(insertData.equals("YES")){
					FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
					frToCassandra.readFastaDirectory(fastaDirectory, numOfSamples, srsSize);
				}
				else if (extractData.equals("YES")){
					CassandraDAO dao = new CassandraDAO();
					for (int i = 1; i <= numOfSamples; i++) {
						System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
						dao.findByFileName(fileNameOutput, i, srsSize);
						Thread.sleep(60000);
					}
				}else{
					CassandraDAO dao = new CassandraDAO();
					System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
					dao.findByID(idSeqDNA);
				}
			}else if (db.equals("MONGODB")){
				if(insertData.equals("YES")){
					for (int i = 1; i <= numOfSamples; i++) {
						MongoDBCreate.main(null);
						System.out.println("******** Repetição: "+i);
						FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
						frToMongo.readFastaDirectory(fastaDirectory, i, srsSize);
					}
				}else{
					MongoDBDAO dao = new MongoDBDAO();
					if (extractData.equals("YES")){
						System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
						dao.findByCollection(fileNameOutput, 0, srsSize);
					}else{
						System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
						dao.findByID(idSeqDNA);
					}

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
		System.out.print("\n******** Tempo total de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");

		return totalTime;
	}

}
