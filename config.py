# config.py
"""
Zentrale Konfiguration für DB-Verbindung und Dateipfade.
"""

DB_PARAMS = {
    "host":     "localhost",
    "dbname":   "DMR_XPath",
    "user":     "postgres",
    "password": "Science_city",
    "port":     "5432",
}

# Pfade zu den XML-Dateien
TOY_XML   = "toy_example.txt"
DBLP_XML  = "dblp.xml"
SMALL_BIB = "my_small_bib.xml"
