package fr.lo;




public class Chapitre {
	
	
	private int id;
	
	private String code;
	
	private String libelle;
	
	private String codeChapitre;
	
	 
	//Constructeurs
	public Chapitre(int id, String code, String libelle, String codeChapitre) {
		super();
		this.id = id;
		this.code = code;
		this.libelle = libelle;
		this.codeChapitre = codeChapitre;
	}
	
	public int getId() {
		return id;
	}
	public String getCode() {
		return code;
	}
	public String getLibelle() {
		return libelle;
	}
	public String getCodeChapitre() {
		return codeChapitre;
	}
	public void setId(int id) {
		this.id = id;
	}
	public void setCode(String code) {
		this.code = code;
	}
	public void setLibelle(String libelle) {
		this.libelle = libelle;
	}
	public void setCodeChapitre(String codeChapitre) {
		this.codeChapitre = codeChapitre;
	}
	
	@Override
	public String toString() {
		return "{" + id + ", " + code + ", " + libelle + "}";
	}

}
