Gruppe M: Finn Ole Hoppe und Moritz Sundermann

Ausführung: Die Container lassen sich mit "docker-compose up -d" starten und der Container datenbankuebung-python_test-1 führt automatisch den Datentransfer in unsere MongoDB sowie die CRUD-Operationen aus.

READ: Die Ergebnisse der Read Operationen werden automatisch in den Logs des "datenbankuebung-python_test-1"-Containers ausgegeben und sind zudem mit den dazugehörigen Code-Snippets in CRUD.md zu finden.

UPDATE und DELETE: Code-Snippets sind wieder in CRUD.md zu finden. Um zu prüfen dass die MongoDB wie gewollt angepasst wurde, haben wir das Tool MongoDBCompass verwendet.

Der vollständige Code zum Datentransfer und den CRUD-Operationen ist in transfer.py, read.py, update.py und delete.py zu finden.

Arbeitsaufteilung:
Python-Service in docker-compose: Finn Ole
MongoDB-Service in docker-compose: Moritz
Python Verbindung zur PostgresDB: Finn Ole
Python Verbindung zur MongoDB: Moritz
Datentransfer von PostgresDB zur MongoDB: Finn Ole
Read A-E: Moritz
Read F-I: Finn Ole
Update: Moritz
Delete: Moritz