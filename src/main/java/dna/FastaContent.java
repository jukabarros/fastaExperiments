package dna;

/**
 * Cria objetos que vem do arquivo fasta inicial, usado para
 * escrever o novo arquivo
 * 
 * Eh usado na extracao para o arquivo fasta
 * @author juccelino.barros
 */
public class FastaContent {
	
	private String id;
	
	private String seqDNA;
	
	private int line;
	
	public FastaContent(String id, String seqDNA, int line) {
		super();
		this.id = id;
		this.seqDNA = seqDNA;
		this.line = line;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getSeqDNA() {
		return seqDNA;
	}

	public void setSeqDNA(String seqDNA) {
		this.seqDNA = seqDNA;
	}

	public int getLine() {
		return line;
	}

	public void setLine(int line) {
		this.line = line;
	}
	

}
