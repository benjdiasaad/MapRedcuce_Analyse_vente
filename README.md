Code Hadoop Java pour le programme MapReduce : Analyse de vente.
==================


Compiler et construire .jar
======

Pour éxecuter vos programmes Hadoop Java, il faudra générer un .jar, puis le copier sur la
machine virtuelle pour l’exécuter sur Hadoop, en premier lieu il faut faire une connexion
SSH avec machine virtuelle, en utilisant un outil très connu c’est PuTTY, deuxièmement
pour transférer votre fichiers qui existent dans votre machine physique vers votre machine
virtuelle il faut utiliser WinSCP.

 1) Utilisation de PuTTY:
 
 
PuTTY : est un émulateur de terminal doublé d'un client pour les protocoles SSH, Telnet, rlogin, et TCP brut. Il permet également des connexions directes par liaison série RS-232. À l'origine disponible uniquement pour Windows, il est à présent porté sur diverses plates-formes Unix.
Après l’installation de PuTTY, lancer l’émulateur et une fenêtre sera déclenché :

<img style="display:inline;" src="https://www.mediafire.com/convkey/a1e7/zmdtmwd5qini1svzg.jpg">

*	Pour connecter avec votre machine virtuelle, il faut taper adresse ip de la machine virtuelle et Port (c'est par défaut =22)

2) Utilisation de WinSCP :

WinSCP : est un client SFTP graphique pour Windows. Il utilise SSH et est open source. Le protocole SCP est également supporté. Le but de ce programme est de permettre la copie sécurisée de fichiers entre un ordinateur local et un ordinateur distant.
Quand on ouvre WinSCP, il déclenche une fenêtre de connexion, il suffit d’entrer l’adresse ip de votre machine virtuelle et port (qui est par défault =2), aussi nom de l’utilisateur = mbds, et password = password.

Après la connexion il nous affiche une fenêtre qui contient deux interfaces, la première qui représente nos fichiers de machine physique et la deuxième interface pour déplacer vos fichiers vers la machine virtuelle.

<img src="https://www.mediafire.com/convkey/36bd/1p830pnzafac31vzg.jpg">


* Sélectionner le fichier qui contient le code java Hadoop (celle-ci de mon repository), et cliquer sur envoyer pour le déplacer vers notre machine virtuelle :

<img src="https://www.mediafire.com/convkey/de7d/repvwhm8t20gl51zg.jpg">

* Compiler le code, avec la commande : javac analyseVente.java :

<img src="https://www.mediafire.com/convkey/d6f8/7zy0ir81ywocqvozg.jpg">


* Construire la hiérarchie du .jar et y déplacer le code compilé :

<img src="https://www.mediafire.com/convkey/6a2c/76ii4bo3ctmv4c5zg.jpg">

<img src="https://www.mediafire.com/convkey/4f88/07ka6wsujbwph55zg.jpg">


* Générer le jar avec la commande : jar -cvf mbds_analysevente.jar -C . org, j'ai nommé le jar "mbds_analysevente" :

<img src="https://www.mediafire.com/convkey/c9b0/6bilr86bs1aw5fczg.jpg">

* Après supprimer le répertoire org avec la commande rm -rf org :

<img src="https://www.mediafire.com/convkey/d936/8x4f1odv5yaeq4jzg.jpg">


* j'ai déplacé le fichier excel 'sales_world_10k.csv', avec la commande : hadoop fs -put sales_world_10k.csv /

<img src="https://www.mediafire.com/convkey/6ac6/d0zhejywblaun2rzg.jpg">

* Vérifiez la présence du fichier sur HDFS, avec la commande : hadoop fs -ls /

<img src="https://www.mediafire.com/convkey/aade/yb8zstmk4a0hevkzg.jpg">

* Exécuter le programme avec la commande : hadoop jar mbds_analysevente.jar org.analyse.vente.analyseVente /sales_world_10k.csv /resultanvente

<img src="https://www.mediafire.com/convkey/4f85/2no5im8rno5mocdzg.jpg">

* Consulter les résultats aprés l’exécution, Taper la commande: hadoop fs -ls /resultanvente

<img src="https://www.mediafire.com/convkey/630a/x571ys5pgxzhenjzg.jpg">

* Ou bien vous pouvez tapez la comande : hadoop fs -cat /resultanvente/*

<img src="https://www.mediafire.com/convkey/7f37/5xk2c96yrvrpypuzg.jpg">

























