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

import create.CassandraCreate;
import dao.CassandraDAO;

public class FastaReaderToCassandra {
	
	private int allLines;
	private CassandraDAO dao;
	
	// Numero da linha de um arquivo especifico
	private int lineNumber;
	
	private File fileTxtCassandra;
	private FileWriter fwCassandra;
	private BufferedWriter bwCassandra;

	public FastaReaderToCassandra() throws IOException {
		super();
		this.allLines = 0;
		this.dao = new CassandraDAO();
		
		this.fileTxtCassandra = null;
		this.fwCassandra = null;
		this.bwCassandra = null;
	}
	
	/**
	 * Ler todos os Fasta de um repositorio especifico
	 * e insere as informacoes do arquivo na tabela fasta_info
	 * @param fastaDirectory
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws InterruptedException 
	 */
	public void readFastaDirectory(String fastaFilePath, int numOfsample, int srsSize) throws SQLException, IOException, InterruptedException{
		File directory = new File(fastaFilePath);
		File[] fList = directory.listFiles();
		// Ordernando a lista por ordem alfabetica
		Arrays.sort(fList);
		// Criando o arquivo txt referente ao tempo de insercao no bd
		this.createTxtFile("INSERT", srsSize);
		this.prepareHeaderInsertTxtFile(fList, "INSERÇÃO");
		
		for (int i = 1; i <= numOfsample; i++) {
			System.out.println("\n******** Amostra: "+i);
			CassandraCreate.main(null);
			for (File file : fList){
				if (file.isFile()){
					if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
						double sizeInMb = file.length() / (1024 * 1024);
						System.out.println("\n* Indexando o arquivo "+file.getName());
						this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
						System.out.println("* Inserindo o conteudo do arquivo no BD");
						this.lineNumber = 0;
						long startTime = System.currentTimeMillis();
						// Inserindo o conteudo no arquivo
						this.insertFastaContent(file.getAbsolutePath(), file.getName(), srsSize);
						long endTime = System.currentTimeMillis();
						this.dao.updateNumOfLinesFastaInfo(file.getName(), lineNumber/2);
	
						String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
						this.bwCassandra.write(timeExecutionSTR + '\t');
						
					}else {
						System.out.println("*** Erro: "+file.getName()+ " não é um arquivo fasta");
					}
				}
			}
			this.bwCassandra.write("\n");
			System.out.println("\n**** Total de linhas inseridas no Banco: "+this.allLines/2);
			Thread.sleep(60000);
			this.allLines = 0;
			this.dao = new CassandraDAO();
		}
		
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
		
	}
	
	/**
	 * Extrai o conteudo do arquivo do BD e monta o arquivo .fa
	 * @param numOfsamples numero de amostra
	 * @param srsSize tamanho da SRS
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void extractData(int numOfsamples, int srsSize) throws IOException, InterruptedException {
		List<String> allFastaFiles = this.dao.getAllFastaFile();
		this.createTxtFile("EXTRACT", srsSize);
		this.prepareHeaderExtractTxtFile(allFastaFiles, "EXTRAÇÃO");
		
		for (int i = 1; i <= numOfsamples; i++) {
			System.out.println("*** Amostra: "+i);

			for (int j = 0; j < allFastaFiles.size(); j++) {
				System.out.println("\n* Extraindo o conteudo de "+allFastaFiles.get(j));
				long startTime = System.currentTimeMillis();
				this.dao.findByFileName(allFastaFiles.get(j), i, srsSize);
				long endTime = System.currentTimeMillis();

				String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
				this.bwCassandra.write(timeExecutionSTR + '\t');
				Thread.sleep(30000);
			}
			this.bwCassandra.write('\n');
			Thread.sleep(60000);
		}
		
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
	}
	
	/**
	 * Consulta pelo id de sequencia, no total são 30 ids consultados.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public void findSeqByID() throws IOException, InterruptedException {
		List<String> allIDs = this.addAllIdSeqs();
		this.createTxtFile("SEARCH", 0);
		this.bwCassandra.write("****** CONSULTA CASSANDRA (segundos) ******\n");
		for (int i = 0; i < allIDs.size(); i++) {
			System.out.println("* Amostra ("+(i+1)+")");
			long startTime = System.currentTimeMillis();
			this.dao.findByID(allIDs.get(i));
			long endTime = System.currentTimeMillis();
			String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
			this.bwCassandra.write(timeExecutionSTR + '\t' + allIDs.get(i)+'\n');
			Thread.sleep(60000);
		}
		
		this.bwCassandra.close();
		this.fwCassandra = null;
		this.fileTxtCassandra = null;
	}
	
	/**
	 * Cria o cabecalho do arquivo de acordo com o diretorio dos arquivos fasta
	 * Experimento INSERCAO
	 * @param fList
	 * @throws IOException
	 */
	private void prepareHeaderInsertTxtFile(File[] fList, String experiment) throws IOException {
		this.bwCassandra.write("****** "+experiment+" CASSANDRA (segundos) ******\n");
		for (File file : fList){
			if (file.isFile()){
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					this.bwCassandra.write(file.getName() + '\t');
				}
			}
		}
		this.bwCassandra.write("\n");
	}
	
	/**
	 * Cria o cabecalho do arquivo de acordo com a tabela fasta_info
	 * Experimento Extracao
	 * @param fList
	 * @throws IOException
	 */
	private void prepareHeaderExtractTxtFile(List<String> allFastaFiles, String experiment) throws IOException {
		this.bwCassandra.write("****** "+experiment+" CASSANDRA (segundos) ******\n");
		for (int i = 0; i < allFastaFiles.size(); i++) {
			this.bwCassandra.write(allFastaFiles.get(i) + '\t');
			
		}
		this.bwCassandra.write("\n");
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
		File[] fList = directory.listFiles();
		Arrays.sort(fList);
		int paramConsult = 0;
		for (File file : fList){
			if (file.isFile()){
				if (file.getName().endsWith(".fasta") || file.getName().endsWith(".fa")){
					
					this.createGrowthSearchTimeTxt(file.getName());
					this.bwCassandra.write("****** CURVA DE CONSULTA ******\n");
					
					long sizeInMb = file.length() / (1024 * 1024);
					
					System.out.println("* Indexando o arquivo "+file.getName());
					this.dao.insertFastaInfo(file.getName(), sizeInMb, "Inserir comentario");
					
					System.out.println("* Inserindo o conteudo do arquivo no BD");
					this.lineNumber = 0;
					this.insertFastaContent(file.getAbsolutePath(), file.getName(), srsSize);
					
					List<String> idSequences = new ArrayList<String>();
					idSequences = this.addAllIdSeqs();
					Thread.sleep(10000); // Aguarda 10 segundos para realizar consulta
					System.out.println("\n\n** Iniciando as Consultas");
					// 5 -> Numero de amostra para o experimento
					for (int i = 0; i < 5; i++) {
						long startTime = System.currentTimeMillis();
						this.dao.findByID(idSequences.get(paramConsult));
						long endTime = System.currentTimeMillis();

						String timeExecutionSTR = this.calcTimeExecution(startTime, endTime);
						this.bwCassandra.write(idSequences.get(paramConsult) + '\t' + "tempo: "+'\t'+timeExecutionSTR+'\n');
						Thread.sleep(2000); // Aguarda 2 segundos para fazer outra consulta
						paramConsult ++;
					}
					this.bwCassandra.close();
					this.fwCassandra = null;
					this.fileTxtCassandra = null;
					// Atualizando o numero de linhas no arquivo
					this.dao.updateNumOfLinesFastaInfo(file.getName(), this.lineNumber/2);
					
				}else {
					System.out.println("*** Atenção "+file.getName()+ " não é um arquivo fasta");
				}
			}
		}
		
		System.out.println("\n\n\n********** FIM ************");
	}
	
	/**
	 * Metodo que adiciona os ids que serao consultados total de 30
	 * @param allIDSeq
	 * @return
	 */
	private List<String> addAllIdSeqs(){
		
		List<String> allIDSeq = new ArrayList<>();
		
		allIDSeq.add(">385_828_1910_F3");
		allIDSeq.add(">375_1783_953_F3");
		allIDSeq.add(">932_31_598_F3");
		allIDSeq.add(">374_1290_504_F3");
		allIDSeq.add(">388_1856_792_F3");
		
		allIDSeq.add(">377_1306_66_F3");
		allIDSeq.add(">381_489_342_F3");
		allIDSeq.add(">1060_1173_984_F3");
		allIDSeq.add(">400_700_648_F3");
		allIDSeq.add(">481_1416_1736_F3");
		
		allIDSeq.add(">476_1737_1136_F3");
		allIDSeq.add(">380_959_822_F3");
		allIDSeq.add(">9999_999_9993");
		allIDSeq.add(">473_1748_181_F3");
		allIDSeq.add(">373_56_358_F3");

		allIDSeq.add(">888888888");
		allIDSeq.add(">935_763_1226_F3");
		allIDSeq.add(">380_968_305_F3");
		allIDSeq.add(">932_1711_642_F3");
		allIDSeq.add(">473_1184_1067_F3");
		
		allIDSeq.add(">1060_1174_4_F3");
		allIDSeq.add(">1078_594_607_F3");
		allIDSeq.add(">379_1585_361_F3");
		allIDSeq.add(">373_246_244_F3");
		allIDSeq.add(">1065_919_326_F3");
		
		allIDSeq.add(">557_2036_1480_F3");
		allIDSeq.add(">746_81_294_F3");
		allIDSeq.add(">560_29_216_F3");
		allIDSeq.add(">929_2036_1706_F3");
		allIDSeq.add(">932_36_394_F3");
		
		return allIDSeq;
	}
	
	/**
	 * Ler um fasta especifico e insere no Cassandra
	 * @param fastaFile
	 * @throws IOException 
	 */
	public void insertFastaContent(String fastaFile, String fastaFileName, int srsSize) throws IOException{
		BufferedReader br = null;
		String line = "";
		String fastaSplitBy = "\n";
		int numOfLine = 0;
		try {
			br = new BufferedReader(new FileReader(fastaFile));
			String idSeq = "";
			String seqDNA = "";
			this.dao.beforeExecuteQuery();
			this.dao.prepareInsert(fastaFileName);
			int allSrsSize = srsSize*2;
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
					// Printando a cada 500 000 registro inseridos
					if (this.lineNumber%1000000 == 0){
						System.out.println("Quantidade de registros inseridos: "+this.lineNumber/2);
					}
				}
			}
			this.dao.afterExecuteQuery();
	 
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
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
		System.out.print("* Tempo de execução: " 
				+ formatter.format(totalTime / 1000d) + " segundos \n");
		
		String totalTimeSTR = formatter.format(totalTime / 1000d);
		return totalTimeSTR;
	}
	
	/**
	 * Cria um arquivo txt que informa o tempo do experimento 
	 * 
	 * @param fastaFile
	 * @param timeExecution
	 * @throws IOException 
	 */
	private void createTxtFile(String experiment, int srsSize) throws IOException{
		this.fileTxtCassandra = new File("cassandra_"+experiment+"_SRS_"+srsSize+".txt");
		this.fwCassandra = new FileWriter(this.fileTxtCassandra.getAbsoluteFile());
		this.bwCassandra = new BufferedWriter(this.fwCassandra);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtCassandra.exists()) {
			this.fileTxtCassandra.createNewFile();
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
	private void createGrowthSearchTimeTxt(String fileName) throws IOException{
		this.fileTxtCassandra = new File("cassandra_"+fileName+"_searchGrowth_.txt");
		this.fwCassandra = new FileWriter(this.fileTxtCassandra.getAbsoluteFile());
		this.bwCassandra = new BufferedWriter(this.fwCassandra);
		
		// if file doesnt exists, then create it
		if (!this.fileTxtCassandra.exists()) {
			this.fileTxtCassandra.createNewFile();
		}
		
	}

}
