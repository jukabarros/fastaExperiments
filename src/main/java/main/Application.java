package main;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Properties;

import config.ReadProperties;
import create.CassandraCreate;
import create.MongoDBCreate;
import create.MySQLCreate;
import dao.CassandraDAO;
import dao.MongoDBDAO;
import dao.MySQLDAO;
import file.FastaReaderToCassandra;
import file.FastaReaderToMongoDB;
import file.FastaReaderToMySQL;

public class Application {

	public long calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo total de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");

		return totalTime;
	}
	/*
	 * Argumentos Opcionais:
	 * 0 - Arquivo ou Diretorio do fasta
	 * 1 - Tamanho da SRS
	 */
	public static void main(String[] args) throws IOException, SQLException, InterruptedException {
		Application app = new Application();
		String fastaDirectory = null;
		int srsSize = 0;
		Properties prop = ReadProperties.getProp();
		String fileNameOutput = prop.getProperty("file.name.output");
		int numOfRepeat = Integer.parseInt(prop.getProperty("num.repeat"));
		int numOfArgs = args.length;
		switch (numOfArgs) {
		case 0:
			System.out.println("** Capturando os parametros no arquivo properties");
			fastaDirectory = prop.getProperty("fasta.directory");
			srsSize = Integer.parseInt(prop.getProperty("srs.quantity"));
			break;
		case 1:
			fastaDirectory = args[0];
			srsSize = Integer.parseInt(prop.getProperty("srs.quantity"));
			break;
		default:
			fastaDirectory = args[0];
			srsSize = Integer.parseInt(args[1]);
			break;
		}
		String allExperiment = prop.getProperty("all.experiments").toUpperCase();
		System.out.println("* Executar todos experimentos: "+allExperiment);
		System.out.println("* Número de Repeticões: "+numOfRepeat);
		System.out.println("* Tamanho da SRS: "+srsSize);
		
		String db = prop.getProperty("database").toUpperCase();
		System.out.println("* Banco de Dados escolhido: "+prop.getProperty("database").toUpperCase());

		String insertData = prop.getProperty("insert.data").toUpperCase();

		String idSeqDNA = prop.getProperty("id.seqDNA");
		String extractData = prop.getProperty("extract.data").toUpperCase();
		String insertAndSearch = prop.getProperty("insert.and.search").toUpperCase();
		long startTime = System.currentTimeMillis();
		/*
		 * INSERINDO / EXTRAINDO DO BD
		 */
		if(db.equals("CASSANDRA")){
			if (allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					System.out.println("***************** Repetição: "+i);
					CassandraCreate.main(null);
					FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
					frToCassandra.doAllExperiment(fastaDirectory, i, srsSize);
					Thread.sleep(45000);
				}

			}else if(insertData.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {

					CassandraCreate.main(null);
					System.out.println("***************** Repetição: "+i);
					if (insertAndSearch.equals("YES")){
						System.out.println("\n**** Realizando a curva de consulta");
						FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
						frToCassandra.readFastaDirectoryAndSearch(fastaDirectory, i, srsSize);
						break;
					}else{
						FastaReaderToCassandra frToCassandra = new FastaReaderToCassandra();
						frToCassandra.readFastaDirectory(fastaDirectory, i, srsSize);
					}
				}

			}else{
				CassandraDAO dao = new CassandraDAO();
				if (extractData.equals("YES")){
					System.out.println("\n**** Extraindo o conteudo de "+fileNameOutput);
					dao.findByFileName(fileNameOutput, 0, srsSize);
				}else{
					System.out.println("\n**** Consultando por id de sequencia: "+idSeqDNA);
					dao.findByID(idSeqDNA);
				}
			}

		}else if (db.equals("MONGODB")){
			if (allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					System.out.println("***************** Repetição: "+i);
					MongoDBCreate.main(null);
					FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
					frToMongo.doAllExperiment(fastaDirectory, i, srsSize);
				}

			}
			else if(insertData.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					MongoDBCreate.main(null);
					System.out.println("******** Repetição: "+i);
					if (insertAndSearch.equals("YES")){
						System.out.println("\n**** Realizando a curva de consulta");
						FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
						frToMongo.readFastaDirectoryAndSearch(fastaDirectory, i, srsSize);
						break;
					}else{
						FastaReaderToMongoDB frToMongo = new FastaReaderToMongoDB();
						frToMongo.readFastaDirectory(fastaDirectory, i, srsSize);
					}
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
			if (allExperiment.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					System.out.println("***************** Repetição: "+i);
					MySQLCreate.main(null);
					FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
					frToMySQL.doAllExperiment(fastaDirectory, i, srsSize);
				}

			}
			else if(insertData.equals("YES")){
				for (int i = 1; i <= numOfRepeat; i++) {
					MySQLCreate.main(null);
					System.out.println("***************** Repetição: "+i);
					if (insertAndSearch.equals("YES")){
						System.out.println("\n**** Realizando a curva de consulta");
						FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
						frToMySQL.readFastaDirectoryAndSearch(fastaDirectory, srsSize);
						break;
					}else{
						FastaReaderToMySQL frToMySQL = new FastaReaderToMySQL();
						frToMySQL.readFastaByDirectory(fastaDirectory, i, srsSize);
					}
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
