package file;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/*
 * Classe responsavel por criar o arquivo fasta apos 
 * a consulta do BD
 */
public class OutputFasta {
	
	private FileWriter fw;
	private File file;
	
	
	public OutputFasta() throws IOException {
		this.fw = null;
		this.file = null;
	}
	
	/**
	 * Cria o arquivo fasta que recebera o conteudo do banco de dados
	 * @param filename nome do arquivo
	 * @throws IOException
	 */
	public void createFastaFile(String filename) throws IOException{
		this.file = new File(filename);
		 
		if (!file.exists()) {
			file.createNewFile();
		}
		this.fw = new FileWriter(this.file.getAbsoluteFile(), true);
	}
	
	
	/**
	 * Cria o arquivo fasta quando a qntd de SRSs por linha eh igual a 1
	 * essa qntd eh definida no arquivo properties: srs.quantity
	 * @param id
	 * @param seqDNA
	 */
	public void writeFastaFile(String id, String seqDNA, int srsSize){
		try {
			if (srsSize > 1){
				String[] brokenStr = id.split(">");
				int breakSeq = 0;
				int seqDNAlength = seqDNA.length()/srsSize;

				for (int i = 1; i <= srsSize; i++) {
					CharSequence uniqueSequenceDNA = seqDNA.subSequence(breakSeq, breakSeq+seqDNAlength);
					this.fw.write(">"+brokenStr[i]+'\n'+uniqueSequenceDNA+'\n');
					breakSeq += seqDNAlength;
				}
				
			}else {
				this.fw.write(id+'\n'+seqDNA+'\n');
			}
		} catch (IOException ex) {
			System.out.println("Erro na criação do arquivo fasta: "+ex.getMessage());
		} 

	}
	
	/**
	 * Fecha o arquivo
	 * @throws IOException
	 */
	public void closeFastaFile() throws IOException{
		this.fw.close();
	}

}
