package laure.cim;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;

//Parcoure le CSV
// Pour chaque ligne 
// cr�� un objet CHAPITRE � partir informations  
// - ID �gal au num�ro de chapitre
// - code A
// - libelle B
// - code chapitre J

// pr�pare l'Objet statement INSERT    	
// ajoute cet objet au batch inserts

// quand arrive au dernier CHAPITRE colonne J = vide
// alors ex�cute batch insert

// Ensuite, pour chaque ligne
// cr�� un objet MALADIE � partir informations 
// - un ID incr�ment�
// - code A
// - libelle court B
// - libelle long C
// - id chapitre (si pr�sent)

// pr�pare l'Objet statement INSERT
// ajoute cet objet au batch inserts
// quand arrive � la derni�re ligne OU quand 1000 lignes sont pass�es
// alors ex�cute batch insert

// pour toutes les lignes ou id_parent n'est pas code chapitre
// trouver l'id correspondant � l'enregistrement (au code)

// Parcoure le CSV
// Cr�� pour chaque ligne un CHAPITRE ajoute statement INSERT 
// quand arrive au dernier CHAPITRE colonne J = vide
// execute le lot
// Cr�� pour chaque ligne un MALADIE ajoute statement INSERT 
// quand arrive � la derni�re ligne OU quand 1000 lignes sont pass�es
// alors ex�cute batch insert

public class TestCSV {

	public static void main(String[] args) throws IOException, CsvException {

		/*
		 * String fileName = "C:\\Users\\Hp\\Documents\\Referentiel_CIM.csv";
		 * 
		 * try (CSVReader csvReader = new CSVReader(new FileReader(fileName))) {
		 * List<String[]> record = new ArrayList<>(); record = csvReader.readAll();
		 * 
		 * record.forEach(x -> System.out.println(Arrays.toString(x))); }
		 * 
		 * }
		 */
		/* Connexion � la base de donn�es */
		String url = "jdbc:mysql://localhost:3308/cim?characterEncoding=utf-8&connectionCollation=utf-8&useUnicode=yes";
		String utilisateur = "root";
		String motDePasse = "1234";
		Connection connexion = null;
		PreparedStatement chapterInsertStmt = null;

		String[] nextRecord = null;

		long start = System.currentTimeMillis();

		try {

			connexion = DriverManager.getConnection(url, utilisateur, motDePasse);

			/* Ici, nous placerons nos requ�tes vers la BDD */
			chapterInsertStmt = connexion.prepareStatement("INSERT INTO CHAPITRE VALUES (?, ?, ?, ?)");

			String file = "C:\\Users\\Hp\\Documents\\Referentiel_CIM.csv";

			FileReader filereader = new FileReader(file);
			CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
			CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(2).withCSVParser(parser).build();

			Integer chapId = 1;

			boolean chapitreEnregistr�s = false;

			while ((nextRecord = csvReader.readNext()) != null) {
				System.out.println(Arrays.toString(nextRecord));
				if (nextRecord[9] != null && !nextRecord[9].isEmpty()) { // si col J n'est ni nulle ni vide
					traiteLigneChapitre(nextRecord, chapId, chapterInsertStmt);
					chapId++;
				} else { // sinon, execute le batch inset CHAPITRE
					if (!chapitreEnregistr�s) {
						chapterInsertStmt.executeBatch();
						chapitreEnregistr�s = true;
					}
					// traiter ici premi�re ligne MALADIE
					// break;
				}
			}

			// ATTENTION nextrecord correspond � la premi�re MALADIE

			csvReader.close();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (connexion != null)
				try {
					/* Fermeture de la connexion */
					connexion.close();
				} catch (SQLException ignore) {
					/* Si une erreur survient lors de la fermeture, il suffit de l'ignorer. */
				}
			if (chapterInsertStmt != null)
				try {
					chapterInsertStmt.close();
				} catch (SQLException ignore) {
					chapterInsertStmt = null;
				}
		}

		System.out.println("Termin� en " + (System.currentTimeMillis() - start) + " msec");

	}

	private static void traiteLigneChapitre(String[] ligne, Integer chapId, PreparedStatement stmt)
			throws SQLException {
		// cr�� un objet CHAPITRE � partir informations
		// - ID �gal au num�ro de chapitre
		// - code A=0
		// - libelle B=1
		// - code chapitre J=9
		Chapitre chap = new Chapitre(chapId, ligne[0], ligne[1], ligne[9]);

		System.out.println(chap);

		// pr�pare l'Objet statement INSERT
		// ajoute cet objet au batch inserts
		stmt.clearParameters();
		stmt.setInt(1, chap.getId());
		stmt.setString(2, chap.getCode());
		stmt.setString(3, chap.getLibelle());
		stmt.setString(4, chap.getCodeChapitre());
		stmt.addBatch();
	}

	

	private static void traiteLigneMaladie(String[] ligne, Integer malId, PreparedStatement stmt) throws SQLException {

	}
}