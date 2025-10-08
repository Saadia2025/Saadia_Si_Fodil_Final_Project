import express from "express";
import cors from "cors";
import sqlite3 from "sqlite3";
import { open } from "sqlite";
import path from "path";
import { fileURLToPath } from "url";

const DB_NAME = "Banks.db";
const TABLE_NAME = "Largest_banks";
const PORT = 3000;

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname)); // sert index.html et autres fichiers statiques

// utilitaire DB
async function getDBConnection() {
  return open({
    filename: DB_NAME,
    driver: sqlite3.Database,
  });
}

// endpoint : tous les pays
app.get("/countries", async (req, res) => {
  try {
    const db = await getDBConnection();
    const rows = await db.all(`SELECT * FROM ${TABLE_NAME} ORDER BY GDP_USD_billions DESC`);
    await db.close();
    console.log(`/countries -> ${rows.length} rows`);
    res.json({ count: rows.length, rows });
  } catch (err) {
    console.error("Error /countries:", err);
    res.status(500).json({ error: "Database error" });
  }
});

// endpoint : pays riches (seuil paramétrable)
app.get("/rich-countries", async (req, res) => {
  try {
    // seuil par défaut = 10000 (milliards USD)
    const threshold = Number(req.query.threshold ?? 10000);
    const db = await getDBConnection();
    // attention : on s'assure que la colonne est comparée numériquement
    const rows = await db.all(
      `SELECT * FROM ${TABLE_NAME} WHERE GDP_USD_billions >= ? ORDER BY GDP_USD_billions DESC`,
      [threshold]
    );
    await db.close();
    console.log(`/rich-countries?threshold=${threshold} -> ${rows.length} rows`);
    res.json({ threshold, count: rows.length, rows });
  } catch (err) {
    console.error("Error /rich-countries:", err);
    res.status(500).json({ error: "Database error" });
  }
});

// simple route d'info/debug (optionnel)
app.get("/debug-counts", async (req, res) => {
  try {
    const db = await getDBConnection();
    const totalObj = await db.get(`SELECT COUNT(*) as c FROM ${TABLE_NAME}`);
    const richObj = await db.get(`SELECT COUNT(*) as c FROM ${TABLE_NAME} WHERE GDP_USD_billions >= 10000`);
    await db.close();
    res.json({ total: totalObj.c, rich_ge_10000: richObj.c });
  } catch (err) {
    console.error("Error /debug-counts:", err);
    res.status(500).json({ error: "Database error" });
  }
});

// page principale
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "index.html"));
});

app.listen(PORT, () => {
  console.log(` Server running at http://localhost:${PORT}`);
});

