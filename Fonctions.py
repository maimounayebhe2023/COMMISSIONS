from datetime import date, timedelta
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

# Fonction de connexion
def get_connection():
    try:
        connexion = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
        )
        return connexion
    except Exception as e:
        print("Connexion échouée :", e)
        return None

# Fonction pour obtenir les dates manquantes
def dates_manquantes(cursor):
    today = date.today()
    last_30_days = [today - timedelta(days=i) for i in range(30, -1, -1)]
    
    #la date la moins recente dans last_30_days
    premier_jour = last_30_days[0]

    # Toutes les dates déjà présentes dans la table
    cursor.execute(""" SELECT distinct CAST(date_encaissement AS DATE) as date_encaissement FROM SUIVI_CORRECTION  WHERE date_encaissement >= ?""", premier_jour)
    rows = cursor.fetchall()
    dates_existantes = {r[0].date() if not isinstance(r[0], date) else r[0] for r in rows if r[0] is not None}

    # les dates à inserer
    dates_a_inserer = [d for d in last_30_days if d not in dates_existantes or d==today]
    return dates_a_inserer

# Fonction pour traitement des commissions
def correction_commissions():
    connexion = get_connection()
    if not connexion:
        return
    cursor = connexion.cursor()
    try:

        # 1Insertion les dates manquantes
        liste_dates = dates_manquantes(cursor)
        for d in liste_dates:
            cursor.execute(
                "INSERT INTO SUIVI_CORRECTION(date_encaissement, statut) VALUES (?, ?)",
                (d, "en attente")
            )
        connexion.commit()

        # Récupérer toutes les dates à traiter 
        cursor.execute(
            "SELECT id, CAST(date_encaissement AS DATE), statut FROM SUIVI_CORRECTION WHERE statut IN ('en attente', 'encours')"
        )
        suivi_map = {row[1]: (row[0], row[2]) for row in cursor.fetchall()}

        #  Traitement par date
        for d, (suivi_id, statut) in suivi_map.items():
            print(f"\n=== Traitement de la date : {d} ===")

            # Mettre le statut à 'encours' 
            if statut != 'encours':
                cursor.execute(
                    "UPDATE SUIVI_CORRECTION SET statut = 'encours' WHERE id = ?",
                    (suivi_id,)
                )
            cursor.execute("""
                DECLARE 
                @NUMERO_QUITTANCE varchar(50),
                @NUM_MOUVEMENT int, 
                @message varchar(80);  
                DECLARE corr_quitt CURSOR FOR   
                select NUMERO_QUITTANCE, NUM_MOUVEMENT
                from reglement r
                where CAST(R.DATEEXPORT AS DATE) = ?

                OPEN corr_quitt;  
                
                FETCH NEXT FROM corr_quitt   INTO @NUMERO_QUITTANCE, @NUM_MOUVEMENT ;
                
                WHILE @@FETCH_STATUS = 0  
                BEGIN  

                    UPDATE REGLEMENT
                    SET COMMISSION_MVT = 
                    (select COMMISSION_AGENCE from QUITTANCIER as Q where NUMERO_QUITTANCE = reglement.NUMERO_QUITTANCE)
                    where NUMERO_QUITTANCE = @NUMERO_QUITTANCE AND NUM_MOUVEMENT = @NUM_MOUVEMENT  --AND COMMISSION_MVT = 0       --DATE_MVT >= '16/04/2015'
                    AND ABS((select SUM(PRIME_TOTAL) from QUITTANCIER as Q where NUMERO_QUITTANCE = reglement.NUMERO_QUITTANCE ) - 
                    (select SUM(PRIME_ENCAISSEE) from REGLEMENT as R where NUMERO_QUITTANCE = reglement.NUMERO_QUITTANCE 
                    and isnull(CODE_ANNULATION,'N') <> 'O' and NUM_MOUVEMENT <= reglement.NUM_MOUVEMENT)) < 1
                    
                    UPDATE REGLEMENT
                    SET ETAT_MVT = 1
                    where NUMERO_QUITTANCE = @NUMERO_QUITTANCE AND NUM_MOUVEMENT = @NUM_MOUVEMENT  --AND COMMISSION_MVT = 0       --DATE_MVT >= '16/04/2015'
                    AND ABS((select SUM(PRIME_TOTAL) from QUITTANCIER as Q where NUMERO_QUITTANCE = reglement.NUMERO_QUITTANCE ) - 
                    (select SUM(PRIME_ENCAISSEE) from REGLEMENT as R where NUMERO_QUITTANCE = reglement.NUMERO_QUITTANCE 
                    and isnull(CODE_ANNULATION,'N') <> 'O' and NUM_MOUVEMENT <= reglement.NUM_MOUVEMENT)) < 1

                    
                    FETCH NEXT FROM corr_quitt  INTO @NUMERO_QUITTANCE, @NUM_MOUVEMENT;

                END   
                CLOSE corr_quitt;
                DEALLOCATE corr_quitt; 
            """, (d,))
            # --- Contrats avant 2023 ---
            cursor.execute("""
                SELECT DISTINCT CONVERT(varchar,R.DATE_VALIDATION,103) AS DATE,
                    R.RECU, code_annulation, CONVERT(varchar,R.DATE_VALIDATION,103) AS DATE_MVT , C.CODE_BRANCHE , a.taux_retenue,
                    R.CODE_AGENCE, LIBELLE_AGENCE,
                    R.NUMERO_POLICE, c.DATE_EFFET_POLICE, LIBELLE_SOUS_BRANCHE,
                    R.NUMERO_QUITTANCE , R.TERME_COMPTANT, R.ETAT_MVT , 
                    Q.PRIME_TOTAL, R.PRIME_ENCAISSEE, r.COMMISSION_MVT,  
                    COMMISSION,(DATEDIFF(month,c.DATE_EFFET_POLICE, R.DATE_MVT_DU)+1) AS "NB_MOIS", a.FAX,
                    (SELECT VALEUR_CARACT FROM vue_MVT_CARACTERISTIQUE WHERE numero_police=c.NUMERO_POLICE AND CODE_CARACTERISTIQUE=21) AS DUREE
                FROM VUE_REGLEMENT_NEW_3 R  
                INNER JOIN QUITTANCIER Q ON Q.NUMERO_QUITTANCE = R.NUMERO_QUITTANCE  
                INNER JOIN AGENCES A ON A.CODE_AGENCE=R.CODE_AGENCE 
                INNER JOIN contrat C ON C.numero_police = Q.numero_police  
                LEFT JOIN FINDEP D ON D.DEPNUMOP = ISNULL(R.CODE_CLIENT_1, 0) 
                INNER JOIN SOUS_BRANCHE S ON S.CODE_SOUS_BRANCHE=C.CODE_SOUS_BRANCHE
                WHERE RTRIM(LTRIM(R.TERME_COMPTANT)) IN ('C', 'T', 'R')    
                    AND R.recu <> '0' 
                    AND RIGHT(RTRIM(NUM_FC),1) ='F'  
                    AND r.CODE_CLIENT_1 IS NULL
                    AND RTRIM(LTRIM(LIBELLE_OPERATION)) = 'REGLEMENT QUITTANCE'
                    AND R.VALIDE = 2  
                    AND R.ETAT_MVT = 1   
                    AND R.COMMISSION_MVT <> 0
                    AND YEAR(DATE_EFFET_POLICE) < 2023
                    AND CAST(R.DATEEXPORT AS DATE) = ?
                    AND R.CODE_AGENCE NOT LIKE '6%' 
                    AND r.CODE_AGENCE NOT LIKE '2%' 
                    AND r.CODE_AGENCE NOT IN (1,516,116)
            """, (d,))
            rows = cursor.fetchall()
            print(f"Quittances avant 2023 : {len(rows)}")

            for row in rows:
                commission_avant = float(row.COMMISSION_MVT)
                prime_totale = float(row.PRIME_TOTAL)
                prime_apres_taxe = prime_totale / 1.02
                NB_MOIS = row.NB_MOIS
                if NB_MOIS <= 12:
                    commission = prime_apres_taxe * 0.30
                elif NB_MOIS <= 24:
                    commission = prime_apres_taxe * 0.20
                elif NB_MOIS <= 36:
                    commission = prime_apres_taxe * 0.10
                else:
                    commission = 0

                cursor.execute("""
                    INSERT INTO details_correc_com
                    (id_Correction, code_agence, numero_quittance, comm_avant, comm_apres)
                    VALUES (?, ?, ?, ?, ?)
                """, (suivi_id, row.CODE_AGENCE, row.NUMERO_QUITTANCE, commission_avant, commission))

                cursor.execute("UPDATE REGLEMENT SET COMMISSION_MVT = ? WHERE NUMERO_QUITTANCE = ?", 
                            (commission, row.NUMERO_QUITTANCE))
                cursor.execute("""
                    UPDATE QUITTANCIER
                    SET COMMISSION_AGENCE = ?, COMMISSION_COMPAGNIE = ?, COMMISSION_PAYE = ?
                    WHERE NUMERO_QUITTANCE = ?
                """, (commission, commission, commission, row.NUMERO_QUITTANCE))

            # --- Contrats à partir de 2023 ---
            cursor.execute("""
                SELECT DISTINCT CONVERT(varchar,R.DATE_VALIDATION,103) AS DATE,
                    R.RECU, code_annulation, CONVERT(varchar,R.DATE_VALIDATION,103) AS DATE_MVT , C.CODE_BRANCHE , a.taux_retenue,
                    R.CODE_AGENCE, LIBELLE_AGENCE,
                    R.NUMERO_POLICE, c.DATE_EFFET_POLICE, LIBELLE_SOUS_BRANCHE,
                    R.NUMERO_QUITTANCE , R.TERME_COMPTANT, R.ETAT_MVT , 
                    Q.PRIME_TOTAL, R.PRIME_ENCAISSEE, r.COMMISSION_MVT,  
                    COMMISSION,(DATEDIFF(month,c.DATE_EFFET_POLICE, R.DATE_MVT_DU)+1) AS "NB_MOIS", a.FAX,
                    (SELECT VALEUR_CARACT FROM vue_MVT_CARACTERISTIQUE WHERE numero_police=c.NUMERO_POLICE AND CODE_CARACTERISTIQUE=21) AS DUREE
                FROM VUE_REGLEMENT_NEW_3 R  
                INNER JOIN QUITTANCIER Q ON Q.NUMERO_QUITTANCE = R.NUMERO_QUITTANCE  
                INNER JOIN AGENCES A ON A.CODE_AGENCE=R.CODE_AGENCE 
                INNER JOIN contrat C ON C.numero_police = Q.numero_police  
                LEFT JOIN FINDEP D ON D.DEPNUMOP = ISNULL(R.CODE_CLIENT_1, 0) 
                INNER JOIN SOUS_BRANCHE S ON S.CODE_SOUS_BRANCHE=C.CODE_SOUS_BRANCHE
                WHERE RTRIM(LTRIM(R.TERME_COMPTANT)) IN ('C', 'T', 'R')    
                    AND R.recu <> '0' 
                    AND RIGHT(RTRIM(NUM_FC),1) ='F'  
                    AND r.CODE_CLIENT_1 IS NULL
                    AND RTRIM(LTRIM(LIBELLE_OPERATION)) = 'REGLEMENT QUITTANCE'
                    AND R.VALIDE = 2  
                    AND R.ETAT_MVT = 1   
                    AND R.COMMISSION_MVT <> 0
                    AND YEAR(DATE_EFFET_POLICE) >= 2023
                    AND CAST(R.DATEEXPORT AS DATE) = ?
                    AND R.CODE_AGENCE NOT LIKE '6%' 
                    AND r.CODE_AGENCE NOT LIKE '2%' 
                    AND r.CODE_AGENCE NOT IN (1,516,116)
            """, (d,))
            rows = cursor.fetchall()
            print(f"Quittances à partir de 2023 : {len(rows)}")

            for row in rows:
                commission_avant = float(row.COMMISSION_MVT)
                NB_MOIS = int(row.NB_MOIS)
                DUREE = int(row.DUREE or 0)
                prime_totale = float(row.PRIME_TOTAL)
                prime_apres_taxe = prime_totale / 1.02

                if NB_MOIS > 36:
                    commission = 0
                elif DUREE <= 20 and NB_MOIS <= 12:
                    commission = prime_apres_taxe * (DUREE / 100)
                elif DUREE <= 20 and NB_MOIS > 12:
                    commission = prime_apres_taxe * (DUREE / 200)
                elif DUREE > 20 and NB_MOIS <= 12:
                    commission = prime_apres_taxe * 0.20
                elif DUREE > 20 and NB_MOIS > 12:
                    commission = prime_apres_taxe * 0.10
                else:
                    commission = 0

                cursor.execute("""
                    INSERT INTO details_correc_com
                    (id_Correction, code_agence, numero_quittance, comm_avant, comm_apres)
                    VALUES (?, ?, ?, ?, ?)
                """, (suivi_id, row.CODE_AGENCE, row.NUMERO_QUITTANCE, commission_avant, commission))

                cursor.execute("UPDATE REGLEMENT SET COMMISSION_MVT = ? WHERE NUMERO_QUITTANCE = ?", 
                            (commission, row.NUMERO_QUITTANCE))
                cursor.execute("""
                    UPDATE QUITTANCIER
                    SET COMMISSION_AGENCE = ?, COMMISSION_COMPAGNIE = ?, COMMISSION_PAYE = ?
                    WHERE NUMERO_QUITTANCE = ?
                """, (commission, commission, commission, row.NUMERO_QUITTANCE))

            # Mettre le statut à terminé après traitement de cette date 
            cursor.execute(
                "UPDATE SUIVI_CORRECTION SET statut = 'terminé' WHERE id = ?",
                (suivi_id,)
            )

            connexion.commit()
    finally:
        if connexion:
            connexion.close()