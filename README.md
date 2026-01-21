# GA4 & Facebook Analytics Dashboard

A unified **real-time analytics dashboard** built with **Flask**, **PostgreSQL**, **Google Analytics 4 (GA4)**, and **Facebook Page Insights**.
The dashboard visualizes user activity on a **Vietnam map**, shows **GA4 active users**, and displays **Facebook daily metrics** (Views, Viewers, Visits, Follows) in a clean, single-screen layout.

---

## ✨ Features

### GA4 Analytics

* Realtime active users (last **5 minutes** & **30 minutes**)
* User distribution on a **heatmap + markers** (Vietnam cities)
* Active users by **device category**
* Views by **page / screen name**

### Facebook Fanpage Analytics

* Daily Facebook metrics:

  * **Views**
  * **Viewers**
  * **Visits**
  * **Follows**
* Data sourced directly from PostgreSQL (`facebook_page_insights_daily`)
* Lightweight layout (no charts / no scrolling)

### Dashboard UI

* Single-screen layout (desktop-friendly)
* Responsive grid layout
* Leaflet-powered interactive map

---

## 🧱 Tech Stack

| Layer         | Technology                        |
| ------------- | --------------------------------- |
| Backend       | Flask (Python)                    |
| Database      | PostgreSQL                        |
| Analytics     | Google Analytics 4 Realtime API   |
| Facebook Data | Facebook Graph API (stored in DB) |
| Frontend      | HTML, CSS Grid, Vanilla JS        |
| Map           | Leaflet + Heatmap                 |

---

## 📁 Project Structure

```
.
├── app.py                 # Flask backend
├── templates/
│   └── index.html         # (if using Flask templates)
├── credentials/
│   └── ga4-sa.json
├── .env
├── .gitignore
└── README.md
```

---

## 🗄️ Database Schema

### facebook_page_insights_daily

```sql
facebook_page_insights_daily (
    id SERIAL PRIMARY KEY,
    metric TEXT,
    report_type TEXT,   -- views | viewers | visits | follows
    value BIGINT,
    date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

---

## ⚙️ Environment Variables

Create a `.env` file:

```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=ga4_analytics
DB_USER=your_db_user
DB_PASSWORD=your_db_password

GA4_PROPERTY_ID=XXXXXXXXX
GOOGLE_APPLICATION_CREDENTIALS=credentials/ga4-sa.json
```

⚠️ **Do NOT commit** `.env` or `ga4-sa.json` to GitHub.

---

## 🚀 Installation & Run

### 1️⃣ Create virtual environment

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 2️⃣ Install dependencies

```bash
pip install flask psycopg2-binary python-dotenv google-analytics-data
```

### 3️⃣ Run the app

```bash
python app.py
```

Access the dashboard:

```
http://localhost:5000
```

---

## 🔌 API Endpoints

### GA4

* `GET /api/active-users`
* `GET /api/map-data`
* `GET /api/users-by-source`
* `GET /api/views-by-page`
* `GET /api/refresh`

### Facebook

* `GET /api/facebook/daily_insights`
* `GET /api/facebook/refresh`

---

## 🔒 Security Notes

* Add the following to `.gitignore`:

```gitignore
.env
credentials/
__pycache__/
venv/
```

* Use **read-only DB users** for production
* Rotate Facebook & Google credentials regularly

---

## 🛠️ Customization Ideas

* Add date range filters for Facebook metrics
* Add weekly/monthly aggregations
* Export reports to CSV
* Add authentication (basic auth / OAuth)

---

## 📜 License

This project is intended for **internal analytics and educational use**.
You are free to modify and adapt it for your own needs.

---

## 🙌 Author

**Billy Bui**
Analytics Dashboard Project
