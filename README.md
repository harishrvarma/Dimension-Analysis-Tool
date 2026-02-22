# Dimension-Analysis-Tool

A Flask-based web application for analyzing product dimensions and detecting outliers in furniture product data using statistical methods (IQR) and machine learning (DBSCAN clustering).

## Features

- **Product Dimension Analysis**: Analyze furniture product dimensions (Height, Width, Depth) to identify anomalies
- **Multiple Detection Algorithms**: 
  - IQR (Interquartile Range) method with configurable multipliers
  - DBSCAN (Density-Based Spatial Clustering) for outlier detection
- **Iterative Analysis**: Support for multiple analysis iterations with history tracking
- **Dynamic Filtering**: Filter products by brand, category, and product type
- **Data Visualization**: Interactive charts and grids for analysis results
- **Database Integration**: SQLAlchemy ORM with Alembic migrations
- **Excel Import**: Import product data from Excel files

## Tech Stack

- **Backend**: Flask (Python)
- **Database**: SQLAlchemy ORM with Alembic migrations
- **Data Analysis**: Pandas, NumPy, Scikit-learn
- **Frontend**: HTML templates with Jinja2

## Project Structure

```
1stop/
├── models/              # Database models
├── repositories/        # Data access layer
├── routes/              # API endpoints
├── services/            # Business logic
├── templates/           # HTML templates
├── migrations/          # Database migrations
├── logs/                # Analysis logs and results
├── app.py               # Application entry point
└── config.py            # Configuration loader
```

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd dimension
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Create a `.env` file with the following variables:
```env
APP_HOST=localhost
APP_PORT=5000
DATABASE_URL=your_database_url
```

5. Run database migrations:
```bash
alembic upgrade head
```

## Usage

1. Start the application:
```bash
python app.py
```

2. Access the web interface at `http://localhost:5000`

3. Import product data via the import interface or use existing data

4. Select product groups, brands, categories, and types to analyze

5. Configure analysis parameters:
   - IQR multipliers for H, W, D dimensions
   - DBSCAN epsilon and minimum samples

6. Run analysis and review outlier detection results

## Analysis Methods

### IQR (Interquartile Range)
Detects outliers based on statistical quartiles with configurable multipliers for each dimension.

### DBSCAN Clustering
Uses density-based clustering to identify products with unusual dimension combinations.

## Database Schema

- **products**: Product information with dimensions
- **product_groups**: Product grouping and categorization
- **Analysis tracking**: Iteration history and outlier status

## License

Proprietary - All rights reserved
