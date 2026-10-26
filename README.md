# CineGuida TV 🎬

**CineGuida TV** è una Web Application leggera e autonoma in Python per consultare tutti i film in programmazione sui canali TV del digitale terrestre italiano (Rai e Mediaset).

Estrae i palinsesti ufficiali in formato JSON direttamente dalle API di **RaiPlay** e **Mediaset Infinity**, li arricchisce con metadati dettagliati da **TMDB** (locandine ad alta risoluzione, backdrop, cast completo, regia, anno, genere, rating) e offre l'integrazione diretta con **Tvheadend** per programmare le registrazioni con un click.

---

## 🌟 Caratteristiche

- **Zero dipendenze esterne pesanti**: sviluppato interamente con la standard library di Python (`http.server`, `urllib`, `sqlite3`, `concurrent.futures`, `json`).
- **Sezioni Organizzate**:
  - 🆕 **Appena Aggiunti**: evidenzia le novità aggiunte ai palinsesti.
  - 📅 **Oggi in TV**: film della giornata corrente con indicatore dinamico `● IN ONDA`.
  - 🗓️ **Programmazione Futura**: suddivisione per singolo giorno (Domani e giorni successivi) + vista completa.
- **Dettagli Film Completi**:
  - Locandina verticale e sfondo backdrop HD
  - Regia, Cast attori, Anno di produzione, Nazione, Genere e Durata
  - Trama completa e valutazione voto
- **Filtri Rapidi**:
  - Per canale / gruppo (Tutti, Solo canali cinema, Rai, Mediaset)
  - Per fascia oraria (Prima Serata 20:30-22:30, Seconda Serata/Notte, Daytime)
  - Ricerca istantanea su titolo, cast, regista o anno
- **Integrazione Tvheadend PVR**:
  - Tasto **`🔴 Registra`** per programmare direttamente la registrazione dell'evento tramite API Tvheadend Digest Auth.
- **Sincronizzazione Automatica**:
  - Cache locale su database SQLite (`cineguida.db`).
  - Worker in background per mantenere aggiornati i palinsesti periodicamente.

---

## 🚀 Installazione e Utilizzo

### Avvio Standalone
```bash
python3 cineguida.py -p 8090
```
L'interfaccia web sarà accessibile all'indirizzo `http://localhost:8090` (o `http://<IP-SERVER>:8090`).

### Opzioni da riga di comando
- `-p, --port <porta>`: specifica la porta HTTP di ascolto (default: `8090`).
- `--no-browser`: disattiva l'apertura automatica del browser all'avvio.
- `--local`: forza l'avvio del server locale anche su ambienti Termux.

---

## 🛠️ Configurazione Systemd (es. Raspberry Pi)

Per avviare CineGuida automaticamente come servizio di sistema:

1. Copiare il file di servizio:
   ```bash
   sudo cp cineguida.service /etc/systemd/system/
   ```
2. Ricaricare i demoni ed abilitare il servizio:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now cineguida.service
   ```
3. Verificare lo stato:
   ```bash
   sudo systemctl status cineguida.service
   ```

---

## 📡 Canali Supportati
- **Rai**: Rai 1, Rai 2, Rai 3, Rai 4, Rai Movie, Rai Premium, Rai 5, Rai Gulp, Rai Yoyo, Rai Storia, Rai Scuola.
- **Mediaset**: Iris, Cine34, 20 Mediaset, Twentyseven (27), Rete 4, Canale 5, Italia 1, La5, Top Crime, Focus, Mediaset Extra, Italia 2, TGCom24.
