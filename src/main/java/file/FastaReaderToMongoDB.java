package file;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import dao.MongoDBDAO;

public class FastaReaderToMongoDB {
	
	public int allLines;
	
	private MongoDBDAO dao;
	
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	/* Sao usadas para criar o arquivo txt indicando
	 * o tempo de insercao de cada arquivo
	 */
	private File fileTxtMongoDB;
	
	private FileWriter fwMongoDB;
	
	private BufferedWriter bwMongoDB;
	
	private List<String> allFilesNames;
	
	public FastaReaderToMongoDB() throws IOException {
		super();
		this.allLines = 0;
		this.lineNumber = 0;
		this.dao = new MongoDBDAO();
		
		this.fileTxtMongoDB = null;
		this.fwMongoDB = null;
		this.bwMongoDB = null;
		this.allFilesNames = new ArrayList<String>();
	}
	
	
	/**
	 * Realiza todos os experimento de uma so vez, na seguinte ordem:
	 * Insere todos os arquivos fastas, consulta por 5 ids diferentes e extrai todos os arquivos
	 * Esse procedimento pode ser repetido de acordo com a propriedade 'num.repeat'
	 * @param fastaFilePath
	 * @param repeat
	 * @param srsSize
	 * @throws SQLException
	 * @throws IOException
	 */
	public void doAllExperiment(String fastaFilePath, int numOfRepeat, int srsSize) throws SQLException, IOException{
		System.out.println("\n\n** Iniciando a Inserção dos arquivos");
		this.readFastaDirectory(fastaFilePath, numOfRepeat, srsSize);
		//cabra4, cabra6 cabra5 cabra6 cabra7
		String[] idSequences = {">557_2036_1480_F3",">746_81_294_F3", ">560_29_216_F3", ">929_2036_1706_F3", ">932_36_394_F3"};
		
		System.out.println("\n\n** Iniciando as Consultas dos arquivos");
//		this.createConsultTimeTxt(repeat, srsSize);
		this.createTxtFile("SEARCH", numOfRepeat, srsSize);
		this.bwMongoDB.write("****** CONSULTA ******\n");
		for (int i = 0; i < idSequences.length; i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByID(idSequences[i]);
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMongoDB.write(idSequences[i] + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
		System.out.println("\n\n** Iniciando a Extração dos arquivos");
//		this.createExtractTimeTxt(repeat,srsSize);
		this.createTxtFile("EXTRACT", numOfRepeat, srsSize);
		this.bwMongoDB.write("****** EXTRAÇÃO ******\n");
		for (int i = 0; i < this.allFilesNames.size(); i++) {
			long startTime = System.currentTimeMillis();
			this.dao.findByCollection(this.allFilesNames.get(i), numOfRepeat, srsSize);
			long endTime = System.currentTimeMillis();

			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwMongoDB.write(this.allFilesNames.get(i) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * Para cada fasta, sera criada uma tabela com o conteudo do arquivo 
	 * e sera registrado na tabela 'fasta_info' o nome do arquivo e suas caracteristicas
	 * @param fastaDirectory
	 * @throws IOException 
	 */
	public void readFastaDirectory(String fastaDirectory, int numOfRepeat, int srsSize) throws IOException{
		File directory = new File(fastaDirectory);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		// Ordernando a lista por ordem alfabetica
		Arrays.sort(fList);
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createTxtFile("INSERT", numOfRepeat, srsSize);
//		this.createInsertTimeTxt(numOfRepeat, srsSize);
		this.bwMongoDB.write("****** INSERÇÃO ******\n");
		for (File file : fList){
			if (file.isFile()){
				System.out.println("Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					// Adicionando os arquivos para fazer a extracao em doAllExperiments
					this.allFilesNames.add(file.getName());
					System.out.println("*** Indexando o arquivo: "+file.getName());
					long sizeInMb = file.length() / (1024 * 1024);
					this.dao.insertFastaInfo(file.getName(), "Inserir comentario", sizeInMb);
					System.out.println("* Inserindo o conteudo do arquivo no BD");
					this.dao.getCollection(file.getName());
					this.lineNumber = 0;
					
					long startTime = System.currentTimeMillis();
					this.readFastaFile(file.getAbsolutePath(), srsSize);
					long endTime = System.currentTimeMillis();
					
					// Atualizando o numero de linhas inseridas no banco
					this.dao.updateNumOfLines(file.getName(), this.lineNumber/2);
					
					// Calculando o tempo de insercao de cada arquivo
					String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
					this.bwMongoDB.write(file.getName() + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
					
				}else {
					System.out.println("*** Atenção: "+file.getName()+ " não é um arquivo .fasta");
				}
			}
		}
		this.bwMongoDB.close();
		this.fwMongoDB = null;
		this.fileTxtMongoDB = null;
		
		System.out.println("**** Fim da Inserção no MongoDB.");
		System.out.println("**** Total de linhas inseridas no Banco: "+this.allLines/2);
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico e realiza a consulta
	 * cada vez que um arquivo é inserido. É usado para fazer a curva de consulta
	 * @param fastaDirectory
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public void readFastaDirectoryAndSearch(String fastaFilePath, int repeat, int srsSize) throws SQLException, IOException, InterruptedException{
		File directory = new File(fastaFilePath);
		//get all the files from a directory
		File[] fList = directory.listFiles();
		// Ordernando a lista por ordem alfabetica
		Arrays.sort(fList);
		
		List<String> idSequences = new ArrayList<String>();
		idSequences = this.addAllIdSeqs(idSequences);
		
		int paramConsult = 0;
		for (File file : fList){
			if (file.isFile()){
				System.out.println("** Lendo o arquivo: "+file.getName());
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					
					this.createAnalistSearchTimeTxt(file.getName());
					this.bwMongoDB.write("****** CURVA DE CONSULTA ******\n");
					
					long sizeInMb = file.length() / (1024 * 1024);
					
					System.out.println("* Indexando o arquivo "+file.getName());
					this.dao.insertFastaInfo(file.getName(), "Inserir comentario", sizeInMb);
					
					System.out.println("* Inserindo o conteudo do arquivo no BD");
					this.dao.getCollection(file.getName());
					this.lineNumber = 0;
					this.readFastaFile(file.getAbsolutePath(), srsSize);
					Thread.sleep(10000); // Aguarda 10 segundos para realizar consulta
					System.out.println("\n\n** Iniciando as Consultas");
					// 5 -> Numero de amostra para o experimento
					for (int i = 0; i < 5; i++) {
						long startTime = System.currentTimeMillis();
						this.dao.findByID(idSequences.get(paramConsult));
						long endTime = System.currentTimeMillis();

						String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
						this.bwMongoDB.write(idSequences.get(paramConsult) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
						Thread.sleep(2000); // Aguarda 10 segundos para fazer outra consulta
						paramConsult++;
					}
					this.bwMongoDB.close();
					this.fwMongoDB = null;
					this.fileTxtMongoDB = null;
					
					// Atualizando o numero de linhas inseridas no banco
					this.dao.updateNumOfLines(file.getName(), this.lineNumber/2);
					
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Metodo que adiciona os ids que serao consultados
	 * @param allIDSeq
	 * @return
	 */
	private List<String> addAllIdSeqs(List<String> allIDSeq){
		// Consulta com 1 milhao
		allIDSeq.add(">385_828_1910_F3"); // Arquivo a1milhao linha 1 500 505
		allIDSeq.add(">375_1783_953_F3"); // Arquivo a1milhao linha 265341
		allIDSeq.add(">932_31_598_F3"); // Nao existe no BD
		allIDSeq.add(">374_1290_504_F3"); // Arquivo a1milhao linha 100 001
		allIDSeq.add(">388_1856_792_F3"); // Arquivo a1milhao linha 1 999 999
		
		// Consulta com 5milhoes
		allIDSeq.add(">377_1306_66_F3"); // Arquivo b5milhoes linha 500 543
		allIDSeq.add(">381_489_342_F3"); // Arquivo b5milhoes linha 965 441
		allIDSeq.add(">1060_1173_984_F3"); // Nao existe no BD 
		allIDSeq.add(">400_700_648_F3"); // Arquivo b5milhoes linha 3 456 777 
		allIDSeq.add(">481_1416_1736_F3"); // Arquivo c10milhoes linha 1 112 223 
		
		// Consulta com 10milhoes
		allIDSeq.add(">476_1737_1136_F3"); // Arquivo c10milhoes linha 455 239
		allIDSeq.add(">380_959_822_F3"); // Arquivo a1milhao linha 865 447
		allIDSeq.add(">9999_999_9993"); // Nao existe no BD
		allIDSeq.add(">473_1748_181_F3"); // Arquivo c10milhoes linha 44 441
		allIDSeq.add(">373_56_358_F3"); // Arquivo a1milhao linha 23

		// Consulta com 15 milhoes
		allIDSeq.add(">888888888"); // Nao existe
		allIDSeq.add(">935_763_1226_F3"); // Arquivo d15 milhoes linha 666 667
		allIDSeq.add(">380_968_305_F3"); // Arquivo b2milhoes linha 865 999
		allIDSeq.add(">932_1711_642_F3"); // Arquivo d15 milhoes linha 99 999
		allIDSeq.add(">473_1184_1067_F3"); // Arquivo c10milhoes linha 5431
		
		// Consulta com 20 milhoes
		allIDSeq.add(">1060_1174_4_F3"); // Arquivo e20milhoes 111
		allIDSeq.add(">1078_594_607_F3"); // Arquivo e20milhoes 3 456 789
		allIDSeq.add(">379_1585_361_F3"); // Arquivo a1milhao linha 777 777
		allIDSeq.add(">373_246_244_F3"); // Arquivo b5milhoes 667
		allIDSeq.add(">1065_919_326_F3"); // Arquivo e20milhoes 1 234 569
		
		return allIDSeq;
	}
	
	/**
	 * Ler um fasta especifico e insere no MongoDB
	 * @param fastaFile
	 * @throws IOException 
	 */
	public void readFastaFile(String fastaFile, int srsSize) throws IOException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			int allSrsSize = srsSize * 2;
			while ((line = br.readLine()) != null) {
				numOfLine++;
				this.allLines++;
				this.lineNumber++;
				String[] brokenFasta = line.split(fastaSplitBy);
				if (numOfLine%2 == 1){
					idSeq += brokenFasta[0];
				}else if (numOfLine > 1){
					seqDNA += brokenFasta[0];
				}
				if (numOfLine%allSrsSize == 0){
					this.dao.insertData(idSeq, seqDNA, this.lineNumber/2);
					idSeq = "";
					seqDNA = "";
				}
				// Printando a cada 500 000 registro inseridos
				if (this.lineNumber%1000000 == 0){
					System.out.println("Quantidade de registros inseridos: "+this.lineNumber/2);
				}
			}
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}  finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	  }

	private String calcTimeExecution (long start, long end){
		long totalTime = end - start;
		NumberFormat formatter = new DecimalFormat("#0.00");
		System.out.print("\n******** Tempo de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");
		
		String totalTimeSTR = formatter.format(totalTime / 1000d)+ " segundos";
		return totalTimeSTR;
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo do experimento 
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createTxtFile(String experiment, int numOfRepeat, int srsSize) throws IOException{
		this.fileTxtMongoDB = new File("mongodb_"+experiment+"_"+numOfRepeat+"_SRS_"+srsSize+".txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
		}
		
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo de consulta cada vez que um arquivo eh inserido
	 * Eh usado durante o experimento de curva de consulta.
	 * A escrita é feita no metodo que lista os diretorios
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createAnalistSearchTimeTxt(String fileName) throws IOException{
		this.fileTxtMongoDB = new File("analistSearch-"+fileName+"_mongoDB_.txt");
		this.fwMongoDB = new FileWriter(this.fileTxtMongoDB.getAbsoluteFile());
		this.bwMongoDB = new BufferedWriter(this.fwMongoDB);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtMongoDB.exists()) {
			this.fileTxtMongoDB.createNewFile();
		}
		
	}
}
