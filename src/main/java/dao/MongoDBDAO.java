package dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;

import create.MongoDBCreate;
import file.OutputFasta;

public class MongoDBDAO {

	private MongoDBCreate mongoDBCreate;
	private MongoCollection<Document> dbCollection;
	
	private List<Document> documents;

	public MongoDBDAO() throws IOException {
		super();
		this.mongoDBCreate = new MongoDBCreate();
		this.documents = new ArrayList<Document>();
	}

	/*
	 * Pega a collection, se nao existir
	 * cria uma nova
	 */
	public void getCollection(String collection) throws IOException{
		System.out.println("* Collection "+collection);
		this.dbCollection = this.mongoDBCreate.getCollection(collection);
	}

	public void insertFastaInfo(String fileName, String comment, long size) throws IOException{
		/**** Insert ****/
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		Document document = new Document();
		document.put("file_name", fileName);
		document.put("size", size);
		document.put("comment", comment);
		document.put("num_lines", 0);
		this.dbCollection.insertOne(document);

		this.dbCollection = null;
	}

	public void insertData(String idSeq, String seqDna, int line) throws IOException{
		/**** Insert ****/
		Document document = new Document();
		document.put("idSeq", idSeq);
		document.put("seqDna", seqDna);
		document.put("line", line);
		this.documents.add(document);
		if (this.documents.size() >= 1000) {
			this.dbCollection.insertMany(documents);
			this.documents.clear();
		}
	}
	
	public void insertLastData() {
		if (this.documents.size() > 0) {
			this.dbCollection.insertMany(documents);
			this.documents = new ArrayList<Document>();
		}
	}
	/**
	 * Atualiza o numero de linhas de um arquivo na colecao fasta_info
	 * @param fileName
	 * @param numOfLines
	 * @throws IOException
	 */
	public void updateNumOfLines(String fileName, int numOfLines) throws IOException{
		/**** Update ****/
		Document newDocument = new Document();
		newDocument.append("$set", new BasicDBObject().append("num_lines", numOfLines));

		BasicDBObject searchQuery = new BasicDBObject().append("file_name", fileName);

		this.mongoDBCreate.getCollection("fasta_info").updateOne(searchQuery, newDocument);
	}

	/**
	 * Retorna o numero de linhas de uma colecao especifica, a fim de verificar
	 * se vai ser necessario realizar consultar por paginacao
	 * @param collection
	 * @throws IOException
	 */
	public int getNumberOfLines(String fileName) throws IOException{
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		Document searchQuery = new Document();
		searchQuery.put("file_name", fileName);
		int numberOfLine = 0;
		for (Document doc : this.dbCollection.find(searchQuery)) {
			numberOfLine = doc.getInteger("num_lines");
			break;
		}
		if (numberOfLine == 0){
			System.out.println("*** Número de linhas igual a 0 :(");
		}
		return numberOfLine;
	}

	/**
	 * Retorna o numero de linhas de uma colecao especifica, a fim de verificar
	 * se vai ser necessario realizar consultar por paginacao
	 * @param collection
	 * @throws IOException
	 */
	public List<String> getAllFastaFile() throws IOException{
		List<String> allFastaFile = new ArrayList<String>();
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		for (Document doc: this.dbCollection.find()) {
			String fileFileName = doc.getString("file_name");
			allFastaFile.add(fileFileName);
		}
		return allFastaFile;
	}

	/**
	 * Retorna o conteudo de uma collection, ou seja,
	 * de um arquivo fasta completo e manda para a lista de Fasta_Info
	 * onde pode ser gerado o arquivo fasta
	 * @param collection
	 * @throws IOException
	 */
	public void findByCollection(String fileName, int numOfSample, int srsSize) throws IOException{
		int numOfLines = this.getNumberOfLines(fileName);
		this.dbCollection = this.mongoDBCreate.getCollection(fileName);
		if (numOfLines != 0){ // Para paginacao colocar "<= 500 000"
			OutputFasta outputFasta = new OutputFasta();
			outputFasta.createFastaFile(numOfSample+fileName);
			for (Document doc: this.dbCollection.find()) {
				outputFasta.writeFastaFile(doc.getString("idSeq"), doc.getString("seqDna"), srsSize);
			}

			System.out.println("* Quantidade de registros: "+numOfLines);
			outputFasta.closeFastaFile();
		} else {
			System.out.println("*** Conteúdo do arquivo não encontrado no Banco de dados :(");
		}
	}

	/**
	 * Procura por um id de sequencia especifico
	 * em todas as collections
	 * @param id
	 * @throws IOException
	 */
	public void findByID(String idSeq) throws IOException{
		Document searchQuery = new Document();
		searchQuery.put("idSeq", idSeq);
		List<String> allFastaCollections = this.getAllFastaFile();
		boolean idSeqFound = false;
		for (int i = 0; i < allFastaCollections.size(); i++) {
			this.dbCollection = this.mongoDBCreate.getCollection(allFastaCollections.get(i));
			
			for (Document doc: this.dbCollection.find(searchQuery)) {
				System.out.println("** ID encontrado na coleção "+allFastaCollections.get(i));
				System.out.println("ID: "+doc.getString("idSeq"));
				System.out.println("Sequência: "+doc.getString("seqDna"));
				System.out.println("Linha: "+doc.getInteger("line"));
			}
		}
		if (!idSeqFound){
			System.out.println("* "+idSeq+" não encontrado no Banco de dados :(");
		}
	}

}
