package laure.cim;

//import javax.persistence.*;

//@Entity
public class Maladie {
	
	// @Id
	// @GeneratedValue
	private int id;
	 
	private String code;
	private String libelleCourt;
	private String libelle;
	private Integer idParent;
	private Integer idChapitre;
	private String parent;
	
	//Constructeur
	public Maladie(int id, String code, String libelleCourt, String libelle, Integer idParent, Integer idChapitre, String parent) {
		super();
		this.id = id;
		this.code = code;
		this.libelleCourt = libelleCourt;
		this.libelle = libelle;
		this.idParent = idParent;
		this.idChapitre = idChapitre;
		this.parent = parent;
	
	}

	public int getId() {
		return id;
	}

	public String getCode() {
		return code;
	}

	public String getLibelleCourt() {
		return libelleCourt;
	}

	public String getLibelle() {
		return libelle;
	}

	public int getIdParent() {
		return idParent;
	}

	public int getIdChapitre() {
		return idChapitre;
	}

	public void setId(int id) {
		this.id = id;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public void setLibelleCourt(String libelleCourt) {
		this.libelleCourt = libelleCourt;
	}

	public void setLibelle(String libelle) {
		this.libelle = libelle;
	}

	public void setIdParent(int idParent) {
		this.idParent = idParent;
	}

	public void setIdChapitre(int idChapitre) {
		this.idChapitre = idChapitre;
	}

	public String getParent() {
		return parent;
	}

	public void setIdParent(Integer idParent) {
		this.idParent = idParent;
	}

	public void setIdChapitre(Integer idChapitre) {
		this.idChapitre = idChapitre;
	}

	public void setParent(String parent) {
		this.parent = parent;
	}
	


}
