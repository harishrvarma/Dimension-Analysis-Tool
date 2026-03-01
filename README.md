# Product Dimension Analysis System

A Flask-based web application for analyzing product dimensions and detecting outliers using statistical methods and machine learning algorithms.

## Features

- Product dimension analysis using IQR and DBSCAN algorithms
- Interactive data visualization with charts and grids
- Outlier detection and classification
- Product matching system
- CSV data import functionality
- Iteration-based analysis workflow
- Database-backed product management

## Tech Stack

- **Backend**: Flask (Python)
- **Database**: SQLAlchemy ORM with Alembic migrations
- **Analysis**: Pandas, NumPy, Scikit-learn
- **Frontend**: HTML templates with Jinja2

## Project Structure

```
├── models/              # Database models
├── repositories/        # Data access layer
├── routes/              # Flask route handlers
├── services/            # Business logic
├── templates/           # HTML templates
├── migrations/          # Database migrations
└── logs/                # Analysis logs and results
```

## Installation

1. Install dependencies:
```bash
pip install flask sqlalchemy alembic pandas numpy scikit-learn python-dotenv
```

2. Configure environment variables in `.env`:
```
APP_NAME=your_app_name
APP_HOST=localhost
APP_PORT=5000
SECRET_KEY=your_secret_key
DATABASE_URL=your_database_url
```

3. Run database migrations:
```bash
alembic upgrade head
```

4. Start the application:
```bash
python app.py
```

## Usage

Access the application at `http://localhost:5000`

### Main Features:

- **Dimension Analysis**: Analyze product dimensions using configurable algorithms
- **Outlier Detection**: Identify products with unusual dimensions
- **Data Import**: Import product data from CSV files
- **Item Matching**: Match products across different systems

## Algorithms

- **IQR (Interquartile Range)**: Statistical method for outlier detection
- **DBSCAN**: Density-based clustering algorithm for identifying outliers

## License

Proprietary
