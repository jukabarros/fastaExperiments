package dao;

import java.io.IOException;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;

import create.MongoDBCreate;
import file.OutputFasta;

public class MongoDBDAO {
	
	private MongoDBCreate mongoDBCreate;
	private DBCollection dbCollection;
	
	public MongoDBDAO() throws IOException {
		super();
		this.mongoDBCreate = new MongoDBCreate();
	}
	
	/*
	 * Pega a collection, se nao existir
	 * cria uma nova
	 */
	public void getCollection(String collection) throws IOException{
		System.out.println("** Collection "+collection);
		this.dbCollection = this.mongoDBCreate.getCollection(collection);
	}
	
	public void insertFastaInfo(String fileName, String comment, long size) throws IOException{
		/**** Insert ****/
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		BasicDBObject document = new BasicDBObject();
		document.put("file_name", fileName);
		document.put("size", size);
		document.put("comment", comment);
		document.put("num_lines", 0);
		this.dbCollection.insert(document);
		
		this.dbCollection = null;
	}
	
	public void insertData(String idSeq, String seqDna, int line) throws IOException{
		/**** Insert ****/
		// create a document to store key and value
		BasicDBObject document = new BasicDBObject();
		document.put("idSeq", idSeq);
		document.put("seqDna", seqDna);
		document.put("line", line);
		this.dbCollection.insert(document);
	}
	
	/**
	 * Atualiza o numero de linhas de um arquivo na colecao fasta_info
	 * @param fileName
	 * @param numOfLines
	 * @throws IOException
	 */
	public void updateNumOfLines(String fileName, int numOfLines) throws IOException{
		/**** Update ****/
		BasicDBObject newDocument = new BasicDBObject();
		newDocument.append("$set", new BasicDBObject().append("num_lines", numOfLines));
	 
		BasicDBObject searchQuery = new BasicDBObject().append("file_name", fileName);
	 
		this.mongoDBCreate.getCollection("fasta_info").update(searchQuery, newDocument);
	}
	
	/**
	 * Retorna o numero de linhas de uma colecao especifica, a fim de verificar
	 * se vai ser necessario realizar consultar por paginacao
	 * @param collection
	 * @throws IOException
	 */
	public int getNumberOfLines(String fileName) throws IOException{
		this.dbCollection = this.mongoDBCreate.getCollection("fasta_info");
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("file_name", fileName);
		int numberOfLine = 0;
		DBCursor cursor = this.dbCollection.find(searchQuery);
		while (cursor.hasNext()) {
		    BasicDBObject obj = (BasicDBObject) cursor.next();
			numberOfLine = obj.getInt("num_lines");
			break;
		}
		if (numberOfLine == 0){
			System.out.println("*** Número de linhas igual a 0 :(");
		}
		return numberOfLine;
	}
	
	/**
	 * Retorna o conteudo de uma collection, ou seja,
	 * de um arquivo fasta completo e manda para a lista de Fasta_Info
	 * onde pode ser gerado o arquivo fasta
	 * @param collection
	 * @throws IOException
	 */
	public void findByCollection(String fileName, int repeat, int srsSize) throws IOException{
		int numOfLines = this.getNumberOfLines(fileName);
		this.dbCollection = this.mongoDBCreate.getCollection(fileName);
		if (numOfLines != 0){ // Para paginacao colocar "<= 500 000"
			OutputFasta outputFasta = new OutputFasta();
			outputFasta.createFastaFile(repeat+fileName);
			DBCursor cursorFind = this.dbCollection.find();
			while (cursorFind.hasNext()) {
				BasicDBObject obj = (BasicDBObject) cursorFind.next();
				outputFasta.writeFastaFile(obj.getString("idSeq"), obj.getString("seqDna"), srsSize);
			}
			System.out.println("\n**** Quantidade de registros: "+numOfLines);
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
		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("idSeq", idSeq);
		Set<String> allCollections = this.mongoDBCreate.listAllColection();
		String[] listAllCollections = allCollections.toArray(new String [allCollections.size()]);
		boolean idSeqFound = false;
		for (int i = 0; i < listAllCollections.length; i++) {
			this.dbCollection = this.mongoDBCreate.getCollection(listAllCollections[i]);
			DBCursor cursor = this.dbCollection.find(searchQuery);
			while (cursor.hasNext()) {
				System.out.println("** ID encontrado na coleção "+listAllCollections[i]);
				BasicDBObject obj = (BasicDBObject) cursor.next();
				System.out.println("ID: "+obj.getString("idSeq"));
				System.out.println("Sequência: "+obj.getString("seqDna"));
				System.out.println("Linha: "+obj.getInt("line"));
				idSeqFound = true;
			}
		}
		if (idSeqFound){
			System.out.println("*** ID "+idSeq+" não encontrado no Banco de dados :(");
		}
	}
	
}
