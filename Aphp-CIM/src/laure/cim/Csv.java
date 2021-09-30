package laure.cim;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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

public class Csv {

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
		String url = "jdbc:mysql://localhost:3308/cim?characterEncoding=utf-8&connectionCollation=utf-8&useUnicode=yes&rewriteBatchedStatements=true";
		String utilisateur = "root";
		String motDePasse = "1234";
		Connection connexion = null;
		PreparedStatement chapterInsertStmt = null;
		PreparedStatement maladieInsertStmt = null;
		String requete = "";

		String[] nextRecord = null;

		long start = System.currentTimeMillis();

		try {

			connexion = DriverManager.getConnection(url, utilisateur, motDePasse);

			// nettoyage
			Statement clear = connexion.createStatement();

			System.out.println("nettoyage");
			requete = "update maladie set id_parent = null;";
			clear.executeUpdate(requete);
			requete = "delete from maladie";
			clear.executeUpdate(requete);
			requete = "delete from chapitre";
			clear.executeUpdate(requete);

			/* Ici, nous placerons nos requ�tes vers la BDD */
			System.out.println("remplissage chapitre");

			chapterInsertStmt = connexion.prepareStatement("INSERT INTO CHAPITRE VALUES (?, ?, ?, ?)");

			String file = "C:\\Users\\Hp\\Documents\\Referentiel_CIM.csv";

			FileReader filereader = new FileReader(file);
			CSVParser parser = new CSVParserBuilder().withSeparator(';').build();
			CSVReader csvReader = new CSVReaderBuilder(filereader).withSkipLines(2).withCSVParser(parser).build();

			Integer chapId = 1;

			boolean chapitreEnregistr�s = false;

			while ((nextRecord = csvReader.readNext()) != null) {
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
					maladieInsertStmt = connexion.prepareStatement("INSERT INTO MALADIE VALUES (?, ?, ?, ?, ?, ?, ?)");

					System.out.println("remplissage maladie");
					
					Integer malId = 1;
					Integer c = 1;
					Integer ltraiter = 1;
					
					traiteLigneMaladie(nextRecord, malId, maladieInsertStmt);
					malId++;
					
					while ((nextRecord = csvReader.readNext()) != null) {
						if (c <= 1000) {
							traiteLigneMaladie(nextRecord, malId, maladieInsertStmt);
							malId++;
							c++;
							ltraiter++;
						} else { // sinon, execute le batch inset MALADIE

							if (c == 1001) {
								traiteLigneMaladie(nextRecord, malId, maladieInsertStmt);
								malId++;
								ltraiter++;
							} else {
								maladieInsertStmt.executeBatch();
								c = 1;
								maladieInsertStmt.clearBatch();
							}

						}
					}
					maladieInsertStmt.executeBatch();
					System.out.println("Insertion de " + ltraiter + " maladies");
				}

			}
			// Jointure sur la bdd
			Statement jointure = connexion.createStatement();

			System.out.println("jointure");
			requete = "update maladie m join chapitre c on m.parent = c.code set m.id_chapitre=c.id;";
			jointure.executeUpdate(requete);
			requete = "update maladie m1 join maladie m2 on m1.parent = m2.code set m1.id_parent=m2.id;";
			jointure.executeUpdate(requete);

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

		// pr�pare l'Objet statement INSERT
		// ajoute cet objet au batch inserts
		stmt.clearParameters();
		stmt.setInt(1, chap.getId());
		stmt.setString(2, chap.getCode());
		stmt.setString(3, chap.getLibelle());
		stmt.setString(4, chap.getCodeChapitre());
		stmt.addBatch();
	}

	Integer malId = 1;

	private static void traiteLigneMaladie(String[] ligne, Integer malId, PreparedStatement stmt) throws SQLException {
		Maladie mal = new Maladie(malId, ligne[0], ligne[1], ligne[2], null, null,ligne[10]);

		//if (ligne[0].equals("A000")) {
			//System.out.println("pause");
		//}
		stmt.clearParameters();
		stmt.setInt(1, mal.getId());
		stmt.setString(2, mal.getCode());
		stmt.setString(3, mal.getLibelleCourt());
		stmt.setString(4, mal.getLibelle());
		stmt.setNull(5, Types.INTEGER);
		stmt.setNull(6, Types.INTEGER);
		stmt.setString(7, ligne[10]);
		stmt.addBatch();
	}
}