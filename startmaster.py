import mariadb
import socket
import threading
import json
import sys
import os

###############################################################
# FONCTION POUR CONNAITRE L'ADR IP¨ DE LA MACHINE
###############################################################
def ip_machine():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(("8.8.8.8",80))
        return s.getsockname()[0]
    except Exception:
        return "127.0.0.1"
    finally:
        s.close()

#########################################################################
# OUVERTURE DE LA DATABASE MASTER
#########################################################################
def ouvre_db_master(ipmaster):

    try:
        connection = mariadb.connect(
            host=ipmaster,
            port=3306,
            user="user_router",
            password="isnotcisco",
            database="routermaster"
        )
        return connection
    except mariadb.Error as err:
        print(f"Erreur de connexion a la BDD MariaDB : {err}")
        sys.exit(1)

#########################################################################
# LECTURE DU FICHIER TOPOLOGIE ET VERIFICATION DE INTEGRITE
#########################################################################
def lecture_topologie():

    nomfichier = "topo.json"

    # Vérification de la présence du fichier de topologie
    if not os.path.exists(nomfichier):
        print(f"[ERREUR] Le fichier '{nomfichier}' est manquant. Impossible de continuer.")
        sys.exit(1)   # Arrêt immédiat du programme

    try :
        with open (nomfichier, "r", encoding="utf-8") as f:
            topologie = json.load(f)
    except json.JSONDecodeError as err:
        print(f"[ERREUR] Le fichier '{nomfichier}' contient un JSON invalide : {err}")
        sys.exit(1)
    except Exception as err:
        print(f"[ERREUR] Impossible de lire le fichier '{nomfichier}' : {err}")
        sys.exit(1)

    for router in topologie:
        nom_routeur = router.get("name")
        routeurs_adj = router.get("adjacents", [])
        if not isinstance(nom_routeur, str):
            raise ValueError (f"routeur sans nom valide : {nom_routeur}")
        if not isinstance(routeurs_adj, list):
            raise ValueError (f"Le format des routeurs adjacents n'est pas une liste pour {nom_routeur}")
        if len(routeurs_adj) > 4:
            raise ValueError (f"Le routeur {nom_routeur} a plus de 4 routeurs adjacents")
    return topologie

#########################################################################
# LECTURE DE LA TOPOLOGIE EN BDD
#########################################################################
def lecture_topologie_db(ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()
    table_topo = []

    cur.execute("SELECT * FROM topologie;")
    for row in cur.fetchall():
        table_topo.append(row)
    cur.close()
    connection.close()
    return table_topo

#########################################################################
# LECTURE DU JOURNAL EN BDD
#########################################################################
def lecture_journal_db(ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()
    table_journal = []

    cur.execute("SELECT * FROM journal;")
    for row in cur.fetchall():
        table_journal.append(row)
    cur.close()
    connection.close()
    return table_journal

#########################################################################
# LECTURE DES ROUTEURS ET CLIENTS EN BDD
#########################################################################
def lecture_routeur_db(ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()
    table_routeur = []

    cur.execute("SELECT * FROM router;")
    for row in cur.fetchall():
        table_routeur.append(row)
    cur.close()
    connection.close()
    return table_routeur

#########################################################################
# IDENTIFICATION DU PORT SUIVANT A UTILISER DANS MARIADB
#########################################################################
def router_port_in_db(ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    cur.execute("SELECT MAX(port) FROM router;")
    (port,) = cur.fetchone()
    if port is None:
        port = 1300
    #print(port)

    cur.close()
    connection.close()
    return port + 1

#########################################################################
# ECRITURE TOPOLOGIE DANS MARIADB
#########################################################################
def ecrire_topologie_db(ipmaster):

    topologie = lecture_topologie()
    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()
    table_topo = []

    #Vide la table de topologie pour rechargement
    cur.execute("TRUNCATE TABLE topologie;")
    connection.commit()

    #Recharge la table de topologie dans MariaDB à partir de la topologie lue dans le fichier json
    for router in topologie:
        nom_routeur = router.get("name")
        routeurs_adj = router.get("adjacents", [])
        if len(routeurs_adj) < 4:
            for i in range(4-len(routeurs_adj)):
                routeurs_adj.append("")
        table_topo.append([nom_routeur, routeurs_adj[0], routeurs_adj[1], routeurs_adj[2], routeurs_adj[3]])
        sql="INSERT INTO topologie (router,adj1, adj2, adj3, adj4) VALUES (?,?,?,?,?);"
        valeurs = (nom_routeur, routeurs_adj[0], routeurs_adj[1], routeurs_adj[2], routeurs_adj[3])
        cur.execute(sql, valeurs)
        connection.commit()

    cur.close()
    connection.close()
    return table_topo

#########################################################################
# REMISE A ZERO DE LA TABLE EN DB DES INFOS ROUTEURS ET LE JOURNAL
#########################################################################
def clear_masterdb_rtr (ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    #Vide la table qui contient les routeurs inscrits pour rechargement
    cur.execute("TRUNCATE TABLE router;")
    connection.commit()

    #Vide la table Journal pour un redémarrage propre.
    cur.execute("TRUNCATE TABLE journal;")
    connection.commit()

    cur.close()
    connection.close()

#########################################################################
# INSCRIPTION DANS LA DB DES INFOS ROUTEUR
#########################################################################
def insc_masterdb_rtr (router_name, ip, port, cle, ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    # inscription du routeur dans la table des routeurs
    sql="INSERT INTO  router (nom, ip, port,clepub) VALUES (?,?, ?,?);"
    valeurs = (router_name, ip, port, cle)
    cur.execute(sql, valeurs)
    connection.commit()

    # lecture de la table pour vérification
    #cur.execute("SELECT * FROM router;")
    #for row in cur.fetchall():
    #    print(row)

    cur.close()
    connection.close()

#########################################################################
# SUPPRESSION DANS LA DB DES INFOS ROUTEUR
#########################################################################
def delete_masterdb_rtr (router_name, ipmaster):
    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    # inscription du routeur dans la table des routeurs
    sql = "DELETE FROM router WHERE nom = ?;"
    valeurs = (router_name,)
    cur.execute(sql, valeurs)
    connection.commit()

    cur.close()
    connection.close()

###############################################################
# AJOUT D'UNE LIGNE DE LOG DANS LE JOURNAL
###############################################################
def ajout_event (sourceeve, evenement, ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    # Ajout de l'evenement dans la table journal
    sql = "INSERT INTO journal (source,event) VALUES (?,?);"
    valeurs = (sourceeve, evenement)
    cur.execute(sql, valeurs)
    connection.commit()

    cur.close()
    connection.close()

###############################################################
# AJOUT d'UN CLIENT DANS TOPOLOGIE
###############################################################
def ajout_client_topo (routeur, nom_client, ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    # inscription du client dans la table de topologie
    sql = "INSERT INTO topologie (router,adj1, adj2, adj3, adj4) VALUES (?,?,?,?,?);"
    valeurs = (nom_client, routeur, "", "", "")
    cur.execute(sql, valeurs)
    connection.commit()

    cur.close()
    connection.close()

###############################################################
# AJOUT d'UN CLIENT DANS TOPOLOGIE
###############################################################
def supp_client_topo ( nom_client, ipmaster):

    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    # Suppression du client dans la table de topologie
    sql = "DELETE FROM topologie WHERE router = ?;"
    valeurs = (nom_client,)
    cur.execute(sql, valeurs)
    connection.commit()

    cur.close()
    connection.close()

###############################################################
# TRAITEMENT INSCRIPTION ROUTEUR
###############################################################
def traite_inscription_rtr(conn, table_topo, ipmaster):

    # On recherche le prochain port qui peut être utilisé par un client ou un routeur
    port_libre = router_port_in_db(ipmaster)
    # print (f"Port libre : {port_libre} et c'est un {type(port_libre)}")

    data = conn.recv(1024)
    if not data :
        print ("no data")
        return
    infortr = json.loads(data.decode())
    # print (f"Client : {infortr}")

    #  inscription du routeur dans la base de donnée des routeurs ou client actifs
    nom_routeur = infortr['router_name']
    ip_router = infortr['ip']
    port_router = port_libre
    cle_pub = infortr['cle']
    insc_masterdb_rtr (nom_routeur, ip_router, port_router, cle_pub, ipmaster)

    # on prépositionne le contenu du dictionnaire avec des adjacents vides
    jsoninfoadj = {
        "port": port_libre,
        "adj1": "",
        "adj2": "",
        "adj3": "",
        "adj4": ""
    }

    # s'il s'agit d'un client, il faut en plus l'inscrire dans la base de topologie
    # car, on ne connait pas en avance à partir de topo.json l'emplacement de connexion d'un client
    # à un routeur.
    if nom_routeur[:2] == "CL":
        ajout_client_topo (cle_pub, nom_routeur, ipmaster)

        jsoninfoadj = {
            "port": port_libre,
            "adj1": cle_pub,
            "adj2": "",
            "adj3": "",
            "adj4": ""
        }

    i = 0
    while i < len(table_topo):
        if table_topo[i][0] == nom_routeur:
            jsoninfoadj = {
                "port": port_libre,
                "adj1": table_topo[i][1],
                "adj2": table_topo[i][2],
                "adj3": table_topo[i][3],
                "adj4": table_topo[i][4]
            }
            break
        i += 1
    # print (f"infoadj en json : {jsoninfoadj}")

    #Envoie des routeurs adjacents au routeur qui vient de s'enregistre
    conn.sendall(json.dumps(jsoninfoadj).encode())

    # on va vérifier qu'on est pas dans une situation ou le routeur n'existe pas dans la topologie
    # Si c'est le cas, on va enregistrer un événement et on va supprimer l'entrée qu'on vient de créer
    # de la table des routeurs/Clients
    if jsoninfoadj["adj1"] == "":
        ajout_event ("Master", f"Routeur {nom_routeur} est inconue dans la topologie", ipmaster)
        print (f"[Erreur] Routeur {nom_routeur} est inconue dans la topologie.")
        delete_masterdb_rtr(nom_routeur, ipmaster)

###############################################################
# TRAITEMENT INSCRIPTION ROUTEUR
###############################################################
def traite_suppression_rtr (conn, ipmaster):

    data = conn.recv(1024)
    if not data :
        print ("no data")
        return
    infortr = json.loads(data.decode())

    # Suppression du routeur dans la base de donnée des routeurs ou client actifs
    nom_routeur = infortr['router_name']
    delete_masterdb_rtr (nom_routeur, ipmaster)

    # s'il s'agit d'un client, il faut en plus l'enlever dans la base de topologie
    if nom_routeur[:2] == "CL":
        supp_client_topo (nom_routeur, ipmaster)

###############################################################
# SERVICE D'ECOUTE D'INSCRIPTION DE ROUTEUR OU CLIENT - Port 5000
###############################################################

def ecoute_inscription_rtr (table_topo, ipmaster):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipmaster, 5000))
    serveur.listen(1)
    serveur.settimeout(1)
    print("Service d'enregistrement actif et en attende de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            # print (f"Connecté à {addr}")
            traite_inscription_rtr(conn, table_topo, ipmaster)
        finally : conn.close()
    serveur.close()

##############################################################################
# SERVICE D'ECOUTE POUR DESINSCRIPTION DE ROUTEUR OU CLIENT - Port 5005
#############################################################################

def ecoute_desinscription_rtr (ipmaster):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipmaster, 5005))
    serveur.listen(1)
    serveur.settimeout(1)
    print("Service de dé-inscription de routeur ou client actif et en attende de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            # print (f"Connecté à {addr}")
            traite_suppression_rtr(conn, ipmaster)
        finally : conn.close()
    serveur.close()

###############################################################
# RECHERCHE DES INFOS ROUTEURS DANS LA BDD
###############################################################
def recherche_rtr_db (rtr, ipmaster):
    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    cur.execute("SELECT ip,port FROM router WHERE nom = ?", (rtr,))
    row = cur.fetchone()
    if row is None:
        print ("Routeur inconnu : ", rtr)
        ajout_event("Master", f"Routeur {rtr} inconnu", ipmaster)
        return None, None

    ipadr, port = row
    # print(ipadr, port)

    cur.close()
    connection.close()
    return ipadr, port

###############################################################
# SERVICE d'INFO POUR ROUTEURS - PORT 5002
# distribue l'adresse IP et le port des routeurs actifs
###############################################################
def service_info_routeur(ipmaster):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipmaster, 5002))
    serveur.listen(1)
    serveur.settimeout(1)
    print("Service d'info routeur actif et en attende de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            # print (f"demande d'info routeur de {addr}")
            data = conn.recv(1024)
            if not data:
                print("no data")
                return
            jsonrtr = json.loads(data.decode())
            rtr = jsonrtr['rtr']
            ipa, port = recherche_rtr_db (rtr, ipmaster)
            if ipa is None:
                conn.sendall("Routeur inconnu".encode())
                ajout_event ("Master", f"Routeur {rtr} inconnu", ipmaster)
            else:
                jsoninfo = {
                    "ip": ipa,
                    "port": port
                }
                # print (f"info routeur en json : {jsoninfo}")
                conn.sendall(json.dumps(jsoninfo).encode())
        finally : conn.close()
    serveur.close()

###############################################################
# SERVICE D'ECOUTE ET D'ENREGISTREMENT D'EVENEMENTS - PORT 5003
# Ce service s'assure que les évènements reçus sur le port
# soient inscrits dans la table journal de la BDD
###############################################################
def service_journal(ipmaster):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipmaster, 5003))
    serveur.listen(5)
    serveur.settimeout(1)
    print("Service d'écoute journal actif et en attende de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            # print (f"evenement de {addr}")
            data = conn.recv(4096)
            if not data:
                print("no data")
                return
            msgjournal = json.loads(data.decode())
            source= msgjournal['source']
            evenement = msgjournal['event']
            ajout_event (source, evenement, ipmaster)

        finally : conn.close()
    serveur.close()

###############################################################
# FONCTION de CONSTRUCTION DU GRAPHE DE TOPOLOGIE
# topo : liste de tuples de la forme
#  (id, nom, adj1, adj2, adj3, adj4)
#
# Retourne un graphe non orienté sous forme de dict :
#  { "R1": {"R2", "R5", "CLA"}, ... }
###############################################################
def construire_graph_topologie(topo):

    graphe = {}

    for ligne in topo:
        # on "dépaquette" le tuple
        _id, noeud, *adjs = ligne   # *adjs récupère les 4 adjacents

        # on enlève les chaînes vides ''
        nouvelle_liste = []
        for a in adjs:
            if a != "":
                nouvelle_liste.append(a)
        adjs = nouvelle_liste

        # S'assurer que le noeud existe dans le dict
        if noeud not in graphe:
            graphe[noeud] = set()

        for adj in adjs:
            # Ajouter l'adjacent côté node
            graphe[noeud].add(adj)

            # S'assurer que l'adjacent existe dans le dict
            if adj not in graphe:
                graphe[adj] = set()

            # Ajouter le lien dans l'autre sens (graphe non orienté)
            graphe[adj].add(noeud)

    return graphe

###############################################################
# FONCTION DE PARCOURS EN LARGEUR DU GRAPHE (Algo type BFS)
###############################################################
def parcours_largeur(graphe, depart, cible):

    if depart not in graphe or cible not in graphe:
        return None

    # La file sera une liste de chemins possibles
    queue = [[depart]]
    vu = {depart}

    while queue:
        # On récupère le premier chemin de la file
        chemin = queue.pop(0)  # <- pop(0) au lieu de popleft()
        noeud = chemin[-1]

        if noeud == cible:
            return chemin

        # On parcourt les voisins du noeud courant
        for voisin in graphe[noeud]:

            if voisin not in vu:
                vu.add(voisin)
                # On crée un nouveau chemin étendu
                nouv_chemin = chemin + [voisin]
                queue.append(nouv_chemin)

    # l'algorithme n'a rien trouvé
    return None

###############################################################
# FONCTION de CALCULE DE ROUTE
###############################################################
def calc_route (source, dest, ipmaster):
    topo = lecture_topologie_db(ipmaster)
    # print ("topo : ", topo)
    graphe = construire_graph_topologie(topo)
    # print ("graphe : ", graphe)
    chemin = parcours_largeur(graphe, source, dest)
    # print ("chemin : ",chemin)
    chemin_inv = []
    if chemin:
        for elem in chemin:
            chemin_inv = [elem] + chemin_inv
        x = chemin_inv.pop()
        # print (chemin_inv)
    return chemin_inv

###############################################################
# RECUPERATION CLES DANS DB
###############################################################
def recherche_cle_db (list_rtr, ipmaster):
    connection = ouvre_db_master(ipmaster)
    cur = connection.cursor()

    reponse = []
    for rtr in list_rtr:
        cur.execute("SELECT nom, clepub FROM router WHERE nom = ?", (rtr,))
        row = cur.fetchone()
        reponse.append(row)
        if row is None:
            print ("Routeur inconnu : ", rtr)
            ajout_event("Master", f"Routeur {rtr} inconnu", ipmaster)
            return None

    cur.close()
    connection.close()
    return reponse

###############################################################
# CONSTRUCTION DE LA STRUCTURE DE ROUTE AVEC CLE ET QTY
###############################################################
def construire_route_cle (source, dest, ipmaster):
    chemin = calc_route (source, dest, ipmaster)
    if chemin :
        cles = recherche_cle_db(chemin, ipmaster)
        #print ("----------------")
        #print (chemin)
        #print (cles)
        reponse = {
            'nbrtr': len(chemin)-1,
            'cles' : cles
        }
        return reponse
    else : return None

###############################################################
# SERVICE D'INFO ROUTES POUR LES CLIENTS - PORT 5001
###############################################################
def service_info_client(ipmaster):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipmaster, 5001))
    serveur.listen(5)
    serveur.settimeout(1)
    print("Service d'info routage client actif et en attende de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            # print (f"demande d'info de route du client {addr}")
            data = conn.recv(1024)
            if not data:
                print("no data")
                return
            jsonreq = json.loads(data.decode())
            sourcecli = jsonreq['source']
            destcli = jsonreq['dest']
            routes = construire_route_cle(sourcecli, destcli, ipmaster)
            # print (f"info route en json : {routes}")
            if routes is None:
                routes={"nbrtr":0,"cles":[]}
            conn.sendall(json.dumps(routes).encode())
        finally : conn.close()
    serveur.close()

##########################################################################################
# PROGRAM PRINCIPAL MASTER
# Le master attend que la base de données mariadb soit sur le même serveur que lui-même
# De plus, il attend que la structure de la base soit crée au préalable.
# La création de la base de données et des tables se fait avec le script masterdbinit.sql
##########################################################################################
stop_event = threading.Event()

if __name__ == "__main__":

    print ("DEMARRAGE PROGRAMME MASTER")
    myip = ip_machine()
    clear_masterdb_rtr(myip)
    table_topo = ecrire_topologie_db(myip)
    ajout_event("Master", f"Démarrage du programme Master", myip)

    # On lance les 5 services en thread qui permettent au master de répondre aux clients et routeurs
    threat_inscr_rtr = threading.Thread(target=ecoute_inscription_rtr, args=(table_topo,myip))
    threat_inscr_rtr.start()
    threat_info_rtr = threading.Thread(target=service_info_routeur, args=(myip,))
    threat_info_rtr.start()
    threat_info_cli = threading.Thread(target=service_info_client, args=(myip,))
    threat_info_cli.start()
    threat_journal = threading.Thread(target=service_journal, args=(myip,))
    threat_journal.start()
    threat_desinsc = threading.Thread(target=ecoute_desinscription_rtr, args=(myip,))
    threat_desinsc.start()

    # On se met en écoute de l'utilisateur et des commandes éventuelles qu'il va saisir jusqu'à la commande quit
    # qui va permettre d'arrêter le master et les threats associées grâce au signal stop_event.
    while True:
        reponse = input("Master Console > ")
        if reponse == "quit":
            stop_event.set() # envoi d'un signal aux threats pour qu'elles s'arrêtent.
            break
        if reponse == "help":
            print ("help : voici la liste des commandes disponibles :")
            print ("quit : arrête le programme master et les threads ")
            print ("topologie : Affiche la topologie actuelle chargée en base de données")
            print ("journal : Affiche le journal d'événements chargée en base de données")
            print ("routeur : Affiche les routeurs et les clients actifs avec leur IP et port d'écoute")
            print ("clear : Vide les tables dynamiques et recharge la topologie du fichier topo.json")
        if reponse == "topologie":
            table_topo = lecture_topologie_db(myip)
            print ("> Topologie actuelle :")
            for ligne in table_topo:
                id, nom, adj1, adj2, adj3, adj4 = ligne
                print(f" routeur/client → [{nom}]  Adjacents → {adj1} {adj2} {adj3} {adj4}")
        if reponse == "journal":
            table_journal = lecture_journal_db(myip)
            print ("> Journal actuel :")
            for ligne in table_journal:
                id, nom, event, dt = ligne
                print(f"{dt.strftime('%Y-%m-%d %H:%M:%S')} → [{nom}] : {event}")
        if reponse == "routeur":
            table_routeur = lecture_routeur_db(myip)
            print ("routeurs et clients actifs avec leurs IP et ports d'écoute :")
            for ligne in table_routeur:
                id, nom, ip, port, clepub, dt = ligne
                print(f"Routeur/client [{nom}] → actif depuis {dt.strftime('%Y-%m-%d %H:%M:%S')} sur IP {ip}:{port}")
        if reponse == "clear":
            clear_masterdb_rtr(myip)
            ecrire_topologie_db(myip)
            print ("La réinitialisation des tables et le rechargement de la topologie du fichier json a été faite")

    print ("SHUTDOWN DU MASTER")
