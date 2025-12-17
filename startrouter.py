import socket
import threading
import json
import sys
import random


###############################################################
# INSCRIPTION D'UN EVENEMENT JOURNAL AUPRES DU MASTER
###############################################################
def inscription_event (masterip, source, event):

    #Construction du message d'évenement
    msgjournal = {
        "source": source,
        "event": event
    }

    # connection au service de journal du master
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        client.connect((masterip, 5003))
        # print("Connecté au service journal.")

        # Envoi des informations nécessaires au master pour enregistrer l'événement
        message = json.dumps(msgjournal).encode()
        client.sendall(message)
        client.close()
        return True
    except Exception:
        print("Impossible de se connecter au service journal du master.")
        return False

###############################################################
# INSCRIPTION ROUTEUR AUPRES DU MASTER
###############################################################
def inscription_routeur (infortr, masterip):

    infoadj = []

    # Connexion au serveur
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try :
        client.connect((masterip, 5000))
        # print("Connecté au serveur.")
        # Envoi des informations nécessaires au master pour enregistrer le routeur
        message = json.dumps(infortr).encode()
        client.sendall(message)
        # Reception des infos du master pour le port à utiliser et les routeurs adjacents
        # dans la conception initiale, on pensait avoir besoin de s'assurer que le routeur connaisse
        # ses routeurs adjacents, mais on n'en a pas besoin pour le moment.
        data = client.recv(1024)
        jsoninfoadj = json.loads(data.decode())

        infoadj.append(jsoninfoadj['port'])
        infoadj.append(jsoninfoadj['adj1'])
        infoadj.append(jsoninfoadj['adj2'])
        infoadj.append(jsoninfoadj['adj3'])
        infoadj.append(jsoninfoadj['adj4'])

        client.close()
        # print("Connexion terminée.")
        return infoadj
    except Exception:
        print("[AVERTISSEMENT] Impossible de se connecter au master.")
        return infoadj

###############################################################
# DE-INSCRIPTION DU ROUTEUR AUPRES DU MASTER
###############################################################
def desinscription_routeur (rtr, masterip):

    infortr = {
        "router_name": rtr
    }
    # Connexion au service de de dé-inscription du master
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try :
        client.connect((masterip, 5005))
        message = json.dumps(infortr).encode()
        client.sendall(message)
        client.close()
        return True
    except Exception:
        print("[AVERTISSEMENT] Impossible de se dé-inscrire auprès du master.")
        return False

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

###############################################################
# FONCTIONS CRYPTAGE / DECRYPTAGE et CALCUL DES CLES
###############################################################
def calcul_cles():
    a1 = random.randint(1, 100)
    a2 = random.randint(1, 100)
    b1 = random.randint(1, 100)
    b2 = random.randint(1, 100)

    m = a1 * b1 -1
    e = a2 * m + a1
    d = b2 * m + b1
    n = (e * d -1) // m
    return e, n, d

def chiffre_message(clepub, msg):
    msgchiffre = []
    for i in range(len(msg)):
        msgchiffre.append(ord(msg[i]) * clepub[0] % clepub[1])
    return msgchiffre

def dechiffre_message(clepriv, msgchiffre):
    msgdechiffre=""
    for i in range(len(msgchiffre)):
        msgdechiffre += chr(msgchiffre[i]*clepriv[0]%clepriv[1])
    return msgdechiffre

###############################################################
# FONCTION ROUTAGE
###############################################################
def routage (conn, routername, cle, masterip):

    # on recoit le message crypté du client
    # on le fait par morceaux car la taille des messages peut devenir important
    bouts = []
    while True:
        data = conn.recv(4096)
        if not data:
            break
        bouts.append(data)

    msg_bytes = b"".join(bouts)
    msgchiffrebrut = msg_bytes.decode()
    msgchiffre = json.loads(msgchiffrebrut)
    msgclient = dechiffre_message(cle, msgchiffre)
    infocli = json.loads(msgclient)
    # print(infocli)
    rtr = infocli['rtr']
    msgclient = infocli['msg']
    conn.close()

    # Connexion au master pour demande d'info prochain saut
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((masterip, 5002))
    #print("Connecté au service d'info routeur.")
    msg = {
        "rtr": rtr  # destination suivante du packet
    }
    message = json.dumps(msg).encode()
    client.sendall(message)
    data = client.recv(1024)
    if not data:
        print("no data")
        return
    infortr = json.loads(data.decode())
    # print(infortr)
    ipa = infortr['ip']
    port = infortr['port']
    client.close()

    # print (f"Destination du message client est : {ipa}:{port}")
    # permet de voir le format (codé ou non pour le dernier saut si c'est vers un client) du message
    print (f"Message client : {msgclient}")

    # Connexion au routeur (ou client) qui est le suivant sur la route
    clientdest = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        clientdest.connect((ipa, port))
        #print("Connecté au routeur ou client destinataire")
        # Si le prochain envoi de message est à destination d'un client final, on s'assure que le message
        # intègre le nom de la source originale du message.
        if rtr[:2] == "CL":
            msgdestdict = {
                "rtr": rtr, # Destination suivante
                "source": infocli['source'],
                "msg": msgclient # payload
            }
            msgdest = json.dumps(msgdestdict)
        else:
            msgdest = msgclient

        messagedest = msgdest.encode()
        clientdest.sendall(messagedest)

        clientdest.close()
        # print("Connexion terminée.")
    except Exception:
        print(f"[ERREUR DE ROUTAGE] Impossible de se connecter au routeur ou client destinataire {rtr}.")
        print ("[AVERTISSEMENT] Le message n'a pas été transmis et est perdu")
        inscription_event(masterip, routername, f"[ERREUR DE ROUTAGE] Impossible de se connecter au routeur ou client destinataire {rtr}.")


###############################################################
# FONCTION ECOUTE COMM DE ROUTAGE
###############################################################
def ecoute_port_rtr(routername, ipadr, cle, masterip):
    serveur = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serveur.bind((ipadr, infoadj[0]))
    serveur.listen(5)
    serveur.settimeout(1)
    print("Serveur actif et en attente de connexion...")

    while not stop_event.is_set():
        try:
            conn, addr = serveur.accept()
        except socket.timeout:
            continue
        try:
            print (f"Connecté à {addr}")
            routage(conn, routername, cle, masterip)
        finally : conn.close()
    serveur.close()

###############################################################
# fonction d'aide en ligne pour le programme
###############################################################
def aide():
    print("")
    print("Usage :")
    print(f"  python startrouteur.py <NomRouteur> <IP_Master>")
    print ("  → Le nom du routeur doit correspondre à un nom se trouvant dans le fichier topo.json du master")
    print ("  → L'adresse IP est celle du serveur qui héberge le programme Master")
    print(f"  [Exemple] → python startrouter.py R1 192.168.1.10")
    print("")
    sys.exit(1)

###############################################################
# PROGRAMME PRINCIPAL ROUTEUR
# Argv[1]  : Nom du routeur à démarrer
# Argv[2] : Adresse IP du serveur qui opère le Master
###############################################################
stop_event = threading.Event()

if __name__ == "__main__":

    # Verification du nombre d'arguments fourni en ligne
    if len(sys.argv) != 3:
        print("[ERREUR] Nombre d'arguments invalide.")
        aide()

    routername = sys.argv[1]
    ipadr = ip_machine()
    masterip = sys.argv[2]

    # Génération des clés privées et publiques du routeur
    e, n, d = calcul_cles()
    clepub = [e, n]
    clepriv = [d, n]

    print(f"DEMMARAGE ROUTEUR {routername} dont l'adresse IP est {ipadr}")

    infortr = {
        "router_name": sys.argv[1],
        "ip": ipadr,
        "cle": clepub
    }

    # On inscrit le routeur auprès du master avec pour but de récupérer le port d'écoute à utiliser
    infoadj = inscription_routeur(infortr, masterip)
    if not infoadj:
        print ("Arrêt du programme pour défaut d'enregistrement auprès du Master")
        sys.exit(1)
    # print(infoadj)
    # print (type(ipadr))
    # print (type(infoadj[1]))

    # gestion du cas de figure où le nom du routeur que l'on cherche à démarrer n'existe pas dans la topologie
    # du master pré-enregistrée. On l'identifie car, tout routeur a au minimum 1 routeur adjacent dans la topologie
    if infoadj[1] == "":
        print(f"[Erreur] le routeur {routername} n'existe pas dans la topologie et ne peux pas interagir avec les autres routeurs.")
        inscription_event(masterip, routername, f"Routeur {routername} inconnu dans la topologie - Démarrage impossible")
        sys.exit(1)

    inscription_event(masterip, routername, "Démarrage du routeur")

    # On lance de threat d'écoute et de traitement des packets entrants/sortant du routeur
    threat_routeur = threading.Thread(target=ecoute_port_rtr, args=(routername, ipadr, clepriv, masterip))
    threat_routeur.start()
    while True:
        reponse = input(f"{routername} Console > ")
        if reponse == "quit":
            stop_event.set()
            break
        if reponse == "help":
            print ("commandes à utiliser sur le routeur : ")
            print ("  help : affiche cette liste de commandes")
            print ("  quit: arrête le routeur ainsi que ses threads")

    desinscription_routeur(routername, masterip)
    inscription_event(masterip, routername, "Arrêt du routeur")
    print ("SHUTDOWN DU ROUTEUR")


