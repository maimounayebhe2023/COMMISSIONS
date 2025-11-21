from datetime import date, timedelta
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()

server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

def get_connection():
    try:
        connexion = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password}"
        )
        return connexion
    except Exception as e:
        print("Connexion échouée :", e)
        return None

def dates_manquantes(cursor):
    today = date.today()
    last_30_days = [today - timedelta(days=i) for i in range(30, -1, -1)]
    
    cursor.execute("SELECT CAST(date_execution AS date) FROM SUIVI_CORRECTION")
    dates_verifi = [row[0] for row in cursor.fetchall()]
    
    dates_a_traiter = [d for d in last_30_days if d not in dates_verifi or d == today]
    return dates_a_traiter

def correction_commissions():
    connexion = get_connection()
    if not connexion:
        return
    cursor = connexion.cursor()
    
    liste_dates = dates_manquantes(cursor)
    
    for d in liste_dates:
        cursor.execute("INSERT INTO SUIVI_CORRECTION(date_execution) VALUES (?)", (d,))
    connexion.commit()
    
    cursor.execute(
        "SELECT id, CAST(date_execution AS date) FROM SUIVI_CORRECTION WHERE CAST(date_execution AS date) IN ({})".format(
            ",".join("?" for _ in liste_dates)
        ),
        liste_dates
    )
    suivi_map = {row[1]: row[0] for row in cursor.fetchall()}
    
    # Traitement par date
    for d in liste_dates:
        suivi_id = suivi_map[d]
        print(f"\n=== Traitement de la date : {d} (id={suivi_id}) ===")
        
        # --- Contrats avant 2023 ---
        cursor.execute("""
           select distinct convert(varchar,R.DATE_VALIDATION,103) as DATE,
            R.RECU, code_annulation, convert(varchar,R.DATE_VALIDATION,103) AS DATE_MVT , C.CODE_BRANCHE , a.taux_retenue,
            R.CODE_AGENCE,LIBELLE_AGENCE,
            R.NUMERO_POLICE, c.DATE_EFFET_POLICE, LIBELLE_SOUS_BRANCHE,
            R.NUMERO_QUITTANCE , R.TERME_COMPTANT, R.ETAT_MVT , 
            Q.PRIME_TOTAL, R.PRIME_ENCAISSEE, r.COMMISSION_MVT,  
            COMMISSION,(DATEDIFF(month,c.DATE_EFFET_POLICE, R.DATE_MVT_DU)+1) as "NB_MOIS", a.FAX,
            (select VALEUR_CARACT from vue_MVT_CARACTERISTIQUE where numero_police=c.NUMERO_POLICE and CODE_CARACTERISTIQUE=21) as DUREE
            from VUE_REGLEMENT_NEW_3 R  
            inner join  QUITTANCIER Q ON Q.NUMERO_QUITTANCE = R.NUMERO_QUITTANCE  
            inner join AGENCES A ON A.CODE_AGENCE=R.CODE_AGENCE 
            inner join contrat C ON C.numero_police = Q.numero_police  
            LEFT JOIN FINDEP D ON D.DEPNUMOP = ISNULL(R.CODE_CLIENT_1, 0) 
            INNER JOIN SOUS_BRANCHE S ON S.CODE_SOUS_BRANCHE=C.CODE_SOUS_BRANCHE
            where rtrim(ltrim(R.TERME_COMPTANT)) in ('C', 'T', 'R')    
            and R.recu <> '0' 
            and RIGHT(RTRIM(NUM_FC) ,1) ='F'  and r.CODE_CLIENT_1 is null
            and RTRIM(LTRIM(LIBELLE_OPERATION)) = 'REGLEMENT QUITTANCE'
            and R.VALIDE = 2  
            and R.ETAT_MVT = 1   and R.COMMISSION_MVT <> 0
            and  year(DATE_EFFET_POLICE) < 2023
            and cast(R.DATEEXPORT as date) = ?
            and R.CODE_AGENCE not like '6%' 
            and r.code_agence not like '2%' and r.CODE_AGENCE not in (1,516, 116)
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
            
            cursor.execute("UPDATE REGLEMENT SET COMMISSION_MVT = ? WHERE NUMERO_QUITTANCE = ?", (commission, row.NUMERO_QUITTANCE))
            cursor.execute("""
                UPDATE QUITTANCIER
                SET COMMISSION_AGENCE = ?, COMMISSION_COMPAGNIE = ?, COMMISSION_PAYE = ?
                WHERE NUMERO_QUITTANCE = ?
            """, (commission, commission, commission, row.NUMERO_QUITTANCE))
        
        # --- Contrats à partir de 2023 ---
        cursor.execute("""
            select distinct convert(varchar,R.DATE_VALIDATION,103) as DATE,
            R.RECU, code_annulation, convert(varchar,R.DATE_VALIDATION,103) AS DATE_MVT , C.CODE_BRANCHE , a.taux_retenue,
            R.CODE_AGENCE,LIBELLE_AGENCE,
            R.NUMERO_POLICE, c.DATE_EFFET_POLICE, LIBELLE_SOUS_BRANCHE,
            R.NUMERO_QUITTANCE , R.TERME_COMPTANT, R.ETAT_MVT , 
            Q.PRIME_TOTAL, R.PRIME_ENCAISSEE, r.COMMISSION_MVT,  
            COMMISSION,(DATEDIFF(month,c.DATE_EFFET_POLICE, R.DATE_MVT_DU)+1) as "NB_MOIS", a.FAX,
            (select VALEUR_CARACT from vue_MVT_CARACTERISTIQUE where numero_police=c.NUMERO_POLICE and CODE_CARACTERISTIQUE=21) as DUREE
            from VUE_REGLEMENT_NEW_3 R  
            inner join  QUITTANCIER Q ON Q.NUMERO_QUITTANCE = R.NUMERO_QUITTANCE  
            inner join AGENCES A ON A.CODE_AGENCE=R.CODE_AGENCE 
            inner join contrat C ON C.numero_police = Q.numero_police  
            LEFT JOIN FINDEP D ON D.DEPNUMOP = ISNULL(R.CODE_CLIENT_1, 0) 
            INNER JOIN SOUS_BRANCHE S ON S.CODE_SOUS_BRANCHE=C.CODE_SOUS_BRANCHE
            where rtrim(ltrim(R.TERME_COMPTANT)) in ('C', 'T', 'R')    
            and R.recu <> '0' 
            and RIGHT(RTRIM(NUM_FC) ,1) ='F'  and r.CODE_CLIENT_1 is null
            and RTRIM(LTRIM(LIBELLE_OPERATION)) = 'REGLEMENT QUITTANCE'
            and R.VALIDE = 2  
            and R.ETAT_MVT = 1   and R.COMMISSION_MVT <> 0
            and  year(DATE_EFFET_POLICE) >= 2023
            and cast(R.DATEEXPORT as date) = ?
            and R.CODE_AGENCE not like '6%' 
            and r.code_agence not like '2%' and r.CODE_AGENCE not in (1,516, 116)
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
            
            cursor.execute("UPDATE REGLEMENT SET COMMISSION_MVT = ? WHERE NUMERO_QUITTANCE = ?", (commission, row.NUMERO_QUITTANCE))
            cursor.execute("""
                UPDATE QUITTANCIER
                SET COMMISSION_AGENCE = ?, COMMISSION_COMPAGNIE = ?, COMMISSION_PAYE = ?
                WHERE NUMERO_QUITTANCE = ?
            """, (commission, commission, commission, row.NUMERO_QUITTANCE))
    
    connexion.commit()
    connexion.close()
    print("Toutes les commissions ont été mises à jour !")


