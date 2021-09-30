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
// créé un objet CHAPITRE à partir informations  
// - ID égal au numéro de chapitre
// - code A
// - libelle B
// - code chapitre J

// prépare l'Objet statement INSERT    	
// ajoute cet objet au batch inserts

// quand arrive au dernier CHAPITRE colonne J = vide
// alors exécute batch insert

// Ensuite, pour chaque ligne
// créé un objet MALADIE à partir informations 
// - un ID incrémenté
// - code A
// - libelle court B
// - libelle long C
// - id chapitre (si présent)

// prépare l'Objet statement INSERT
// ajoute cet objet au batch inserts
// quand arrive à la dernière ligne OU quand 1000 lignes sont passées
// alors exécute batch insert

// pour toutes les lignes ou id_parent n'est pas code chapitre
// trouver l'id correspondant à l'enregistrement (au code)

// Parcoure le CSV
// Créé pour chaque ligne un CHAPITRE ajoute statement INSERT 
// quand arrive au dernier CHAPITRE colonne J = vide
// execute le lot
// Créé pour chaque ligne un MALADIE ajoute statement INSERT 
// quand arrive à la dernière ligne OU quand 1000 lignes sont passées
// alors exécute batch insert

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
		/* Connexion à la base de données */
		String url = "jdbc:mysql://localhost:3308/cim?characterEncoding=utf-8&connectionCollation=utf-8&useUnicode=yes";
		String utilisateur = "root";
		String motDePasse = "1234";
		Connection connexion = null;
		PreparedStatement chapterInsertStmt = null;

		String[] nextRecord = null;

		long start = System.currentTimeMillis();

		try {

			connexion = DriverManager.getConnection(url, utilisateur, motDePasse);

			/* Ici, nous placerons nos requêtes vers la BDD */
			chapterInsertStmt = connexion.prepareStatement("INSERT INTO CHAPITRE VALUES (?, ?, ?, ?)");

			String file = "C:\\Users\\Hp\\Documents\\Referentiel_CIM.csv";

			FileReader filereader = new FileReader(file);
			CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
			CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(2).withCSVParser(parser).build();

			Integer chapId = 1;

			boolean chapitreEnregistrés = false;

			while ((nextRecord = csvReader.readNext()) != null) {
				System.out.println(Arrays.toString(nextRecord));
				if (nextRecord[9] != null && !nextRecord[9].isEmpty()) { // si col J n'est ni nulle ni vide
					traiteLigneChapitre(nextRecord, chapId, chapterInsertStmt);
					chapId++;
				} else { // sinon, execute le batch inset CHAPITRE
					if (!chapitreEnregistrés) {
						chapterInsertStmt.executeBatch();
						chapitreEnregistrés = true;
					}
					// traiter ici première ligne MALADIE
					// break;
				}
			}

			// ATTENTION nextrecord correspond à la première MALADIE

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

		System.out.println("Terminé en " + (System.currentTimeMillis() - start) + " msec");

	}

	private static void traiteLigneChapitre(String[] ligne, Integer chapId, PreparedStatement stmt)
			throws SQLException {
		// créé un objet CHAPITRE à partir informations
		// - ID égal au numéro de chapitre
		// - code A=0
		// - libelle B=1
		// - code chapitre J=9
		Chapitre chap = new Chapitre(chapId, ligne[0], ligne[1], ligne[9]);

		System.out.println(chap);

		// prépare l'Objet statement INSERT
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