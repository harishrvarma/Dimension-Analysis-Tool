# Product Dimension Analysis System

A comprehensive Flask-based web application for analyzing product dimensions, detecting outliers, and managing product data using advanced statistical methods and machine learning algorithms.

## 📋 Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Project Architecture](#project-architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Setup](#database-setup)
- [Usage Guide](#usage-guide)
- [Analysis Algorithms](#analysis-algorithms)
- [API Endpoints](#api-endpoints)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

The Product Dimension Analysis System is designed to help businesses analyze product dimensions (Height, Width, Depth) across large product catalogs. It identifies outliers, groups similar products, and provides insights through interactive visualizations. The system supports iterative analysis workflows, allowing users to refine their analysis over multiple iterations.

### Use Cases

- **E-commerce**: Validate product dimension data for consistency
- **Inventory Management**: Identify products with unusual dimensions
- **Quality Control**: Detect data entry errors in product specifications
- **Product Matching**: Match products across different systems based on dimensions

## ✨ Key Features

### 1. Multi-Algorithm Outlier Detection

- **IQR (Interquartile Range)**: Statistical method for detecting outliers based on quartile distribution
- **DBSCAN**: Density-based clustering algorithm for identifying anomalous products
- **Configurable Parameters**: Adjust multipliers, epsilon, and minimum samples for fine-tuned analysis

### 2. Iterative Analysis Workflow

- Perform multiple analysis iterations on the same dataset
- Analyze all products or only normal products from previous iterations
- Track analysis history with detailed iteration logs
- Reset and restart analysis at any point

### 3. Interactive Data Visualization

- **Charts**: Visual representation of dimension distributions
- **Data Grids**: Tabular view with sorting and filtering capabilities
- **Outlier Highlighting**: Color-coded identification of anomalous products
- **Cluster Visualization**: View DBSCAN clustering results

### 4. Product Management

- Import product data from CSV files
- Group products by brand, category, and type
- Manual outlier marking and override capabilities
- Bulk processing of product combinations

### 5. Item Matching System

- Match products across different systems
- Configurable matching attributes and scoring
- Support for competitor product matching
- Attribute-based similarity scoring

## 🛠 Technology Stack

### Backend
- **Flask**: Web framework for Python
- **SQLAlchemy**: ORM for database operations
- **Alembic**: Database migration management
- **Python 3.x**: Core programming language

### Data Analysis
- **Pandas**: Data manipulation and analysis
- **NumPy**: Numerical computing
- **Scikit-learn**: Machine learning algorithms (DBSCAN, StandardScaler)

### Database
- **MySQL/PostgreSQL**: Relational database (configurable)
- **SQLAlchemy ORM**: Database abstraction layer

### Frontend
- **Jinja2**: Template engine for HTML rendering
- **HTML/CSS**: User interface

## 🏗 Project Architecture

The application follows a layered architecture pattern:

```
┌─────────────────────────────────────┐
│         Presentation Layer          │
│    (Flask Routes + Templates)       │
├─────────────────────────────────────┤
│         Business Logic Layer        │
│          (Services)                 │
├─────────────────────────────────────┤
│         Data Access Layer           │
│        (Repositories)               │
├─────────────────────────────────────┤
│         Database Layer              │
│    (SQLAlchemy Models + MySQL)      │
└─────────────────────────────────────┘
```

### Design Patterns

- **Repository Pattern**: Separates data access logic from business logic
- **Service Layer**: Encapsulates business logic and orchestrates operations
- **MVC Pattern**: Separates concerns between models, views, and controllers

## 📦 Installation

### Prerequisites

- Python 3.8 or higher
- MySQL or PostgreSQL database
- pip (Python package manager)

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd harish
```

### Step 2: Create Virtual Environment

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install flask sqlalchemy alembic pandas numpy scikit-learn python-dotenv pymysql
```

### Required Packages

```
flask>=2.3.0
sqlalchemy>=2.0.0
alembic>=1.11.0
pandas>=2.0.0
numpy>=1.24.0
scikit-learn>=1.3.0
python-dotenv>=1.0.0
pymysql>=1.1.0
```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project root:

```env
# Application Settings
APP_NAME=ProductAnalysisSystem
APP_HOST=localhost
APP_PORT=5000
SECRET_KEY=your-secret-key-here

# Database Configuration
DATABASE_URL=mysql+pymysql://username:password@localhost:3306/database_name

# Optional Settings
DEBUG=True
LOG_LEVEL=INFO
```

### Configuration File

The `config.py` file loads environment variables automatically:

```python
from dotenv import load_dotenv
load_dotenv()
```

## 🗄 Database Setup

### Step 1: Create Database

```sql
CREATE DATABASE product_analysis CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### Step 2: Run Migrations

```bash
# Initialize Alembic (if not already done)
alembic init migrations

# Run all migrations
alembic upgrade head
```

### Database Schema

The system uses the following main tables:

- **product**: Stores product information and dimension data
- **product_group**: Groups products by category
- **dimension_product_iteration**: Tracks analysis iterations
- **dimension_product_iteration_item**: Stores iteration results per product
- **matching_***: Tables for product matching functionality

### Migration Files

Located in `migrations/versions/`:
- `34c5e4d31996_create_product_and_product_group_table.py`
- `64a861eb7441_create_product_iteration_table.py`
- `755823327965_item_matching_tables.py`
- And more...

## 🚀 Usage Guide

### Starting the Application

```bash
python app.py
```

Access the application at: `http://localhost:5000`

### Workflow Overview

#### 1. Import Product Data

Navigate to **Database > Import CSV** and upload your product data file.

**CSV Format Requirements:**
```csv
qb_code,brand,category,product_type,name,height,width,depth,weight,base_image_url,product_url
SKU001,BrandA,Furniture,Chair,Office Chair,100,50,50,10,http://...,http://...
```

#### 2. Configure Analysis Parameters

- **Select Product Group**: Choose the product group to analyze
- **Filter by Brand**: Select one or more brands
- **Filter by Category**: Choose product category
- **Filter by Type**: Select specific product types (optional)

#### 3. Choose Analysis Algorithm

**IQR (Interquartile Range)**
- Adjust multipliers for H, W, D dimensions (default: 1.5)
- Lower multiplier = stricter outlier detection
- Higher multiplier = more lenient detection

**DBSCAN**
- **Epsilon (eps)**: Maximum distance between points in a cluster (default: 1.0)
- **Min Samples**: Minimum points required to form a cluster (default: 4)

#### 4. Run Analysis

Click **Analyze** to process the data. The system will:
1. Load products based on filters
2. Apply selected algorithms
3. Identify outliers and normal products
4. Display results in charts and grids

#### 5. Review Results

- **Charts**: Visual distribution of dimensions
- **Grid View**: Detailed product list with outlier flags
- **Statistics**: Total products, outliers, normals, percentages

#### 6. Iterative Refinement

- **Save Iteration**: Save current analysis results
- **Analyze Normals**: Run analysis only on normal products from previous iteration
- **Manual Override**: Mark specific products as outliers manually
- **Reset**: Clear all iterations and start fresh

### Advanced Features

#### Bulk Processing

Process multiple brand-category combinations automatically:

1. Navigate to **Dimension > Analyzer**
2. Select filters
3. Click **Generate Combinations**
4. Review and process combinations in batch

#### Manual Outlier Marking

1. Select products in the grid
2. Click **Mark as Outlier**
3. System updates status with `outlier_mode=1` (manual)

#### Cluster Analysis

When using DBSCAN:
- Products are grouped into clusters
- Cluster -1 indicates outliers/noise
- Click on clusters to view grouped products

## 🧮 Analysis Algorithms

### IQR (Interquartile Range)

**How it works:**
1. Calculate Q1 (25th percentile) and Q3 (75th percentile) for each dimension
2. Compute IQR = Q3 - Q1
3. Define bounds: 
   - Lower Bound = Q1 - (multiplier × IQR)
   - Upper Bound = Q3 + (multiplier × IQR)
4. Products outside bounds are marked as outliers

**Advantages:**
- Simple and interpretable
- Works well with normally distributed data
- Configurable sensitivity via multipliers

**Use Cases:**
- Initial data quality checks
- Detecting extreme values
- Per-dimension outlier detection

### DBSCAN (Density-Based Spatial Clustering)

**How it works:**
1. Standardize dimension data (H, W, D)
2. For each point, find neighbors within epsilon distance
3. Points with >= min_samples neighbors form clusters
4. Points with fewer neighbors are marked as outliers

**Advantages:**
- Discovers clusters of arbitrary shape
- Robust to outliers
- No need to specify number of clusters

**Parameters:**
- **eps**: Smaller values = tighter clusters, more outliers
- **min_samples**: Higher values = denser clusters required

**Use Cases:**
- Finding groups of similar products
- Detecting anomalous dimension combinations
- Multi-dimensional outlier detection

### Combined Analysis

When both algorithms are selected:
- Products must be outliers in BOTH algorithms to be marked as final outliers
- Provides more conservative outlier detection
- Reduces false positives

## 🔌 API Endpoints

### Dimension Analysis

```
GET  /dimension/analyzer              - Analysis interface
POST /dimension/analyzer/analyze      - Run analysis
POST /dimension/analyzer/save         - Save iteration
POST /dimension/analyzer/reset        - Reset iterations
```

### Data Management

```
GET  /database/import                 - Import interface
POST /database/import/upload          - Upload CSV file
```

### Charts and Grids

```
GET  /dimension/chart                 - Chart visualization
GET  /dimension/grid                  - Grid view
POST /dimension/outlier/mark          - Mark outliers manually
```

### Item Matching

```
GET  /item-match                      - Matching interface
POST /item-match/process              - Process matching
```

## 📁 Project Structure

```
harish/
├── app.py                      # Application entry point
├── config.py                   # Configuration loader
├── constants.py                # Application constants
├── .env                        # Environment variables
├── alembic.ini                 # Alembic configuration
│
├── models/                     # Database models
│   ├── base/                   # Base model classes
│   │   ├── base.py            # SQLAlchemy Base
│   │   ├── base_model.py      # Base model with common fields
│   │   └── base_repository.py # Base repository pattern
│   ├── dimension/             # Dimension analysis models
│   │   ├── product_iteration.py
│   │   └── product_iteration_item.py
│   ├── matching/              # Product matching models
│   ├── product.py             # Main product model
│   └── product_group.py       # Product group model
│
├── repositories/              # Data access layer
│   ├── dimension/
│   │   ├── product_iteration_repository.py
│   │   └── product_iteration_item_repository.py
│   ├── product_repository.py
│   └── product_group_repository.py
│
├── services/                  # Business logic layer
│   ├── dimension/
│   │   ├── analyzer.py       # Core analysis logic
│   │   ├── chart.py          # Chart generation
│   │   ├── grid.py           # Grid data processing
│   │   └── outlier.py        # Outlier detection
│   └── item_match/           # Matching algorithms
│       ├── algorithms.py
│       ├── matcher.py
│       └── score_service.py
│
├── routes/                    # Flask route handlers
│   ├── dimension/
│   │   ├── analyzer.py       # Analysis routes
│   │   ├── chart.py          # Chart routes
│   │   ├── grid.py           # Grid routes
│   │   └── outlier.py        # Outlier routes
│   ├── database/
│   │   └── import_csv.py     # CSV import routes
│   ├── index.py              # Main routes
│   └── item_match.py         # Matching routes
│
├── templates/                 # HTML templates
│   ├── base.html             # Base template
│   ├── home.html             # Home page
│   ├── dimension/            # Analysis templates
│   ├── database/             # Import templates
│   └── item_match/           # Matching templates
│
├── migrations/                # Database migrations
│   ├── versions/             # Migration scripts
│   ├── env.py               # Migration environment
│   └── script.py.mako       # Migration template
│
├── logs/                      # Analysis logs
│   ├── analysis.log          # Application logs
│   ├── analysis_results.csv  # Analysis results
│   ├── outlier_items.csv     # Outlier products
│   └── normal_items.csv      # Normal products
│
└── var/                       # Variable data
    └── items/                # CSV data files
```

## 🔧 Key Components Explained

### Models Layer

**Purpose**: Define database schema and ORM mappings

**Key Files**:
- `product.py`: Main product entity with dimensions, status fields
- `product_iteration.py`: Tracks analysis iterations with parameters
- `product_iteration_item.py`: Stores per-product iteration results

### Repositories Layer

**Purpose**: Abstract database operations and queries

**Key Methods**:
- `load_products_filtered()`: Load products with filters
- `get_brands_for_group()`: Get available brands
- `update_products_aggregated()`: Bulk update product status
- `save_iteration_results()`: Save analysis iteration

### Services Layer

**Purpose**: Implement business logic and algorithms

**Key Functions**:
- `analyze_products()`: Main analysis orchestration
- `calculate_iqr_bounds()`: IQR calculation
- `detect_outliers_dbscan()`: DBSCAN clustering
- `save_iteration_to_db()`: Persist iteration results

### Routes Layer

**Purpose**: Handle HTTP requests and responses

**Pattern**: Each route file corresponds to a feature area
- Validates input parameters
- Calls service layer functions
- Returns JSON responses or renders templates

## 📊 Data Flow

### Analysis Workflow

```
User Request
    ↓
Route Handler (routes/dimension/analyzer.py)
    ↓
Service Layer (services/dimension/analyzer.py)
    ├─→ Load Data (repositories/product_repository.py)
    ├─→ Apply IQR Algorithm
    ├─→ Apply DBSCAN Algorithm
    ├─→ Combine Results
    └─→ Save Iteration (repositories/dimension/)
    ↓
Update Product Table (aggregated status)
    ↓
Return Results to User
```

### Iteration Management

1. **First Iteration**: Analyze all products from `product` table
2. **Subsequent Iterations**: Analyze products from previous iteration based on mode
3. **Aggregation**: Calculate final status across all iterations
4. **Product Table Update**: Update `dbs_status`, `iqr_status`, `final_status`, `outlier_mode`

## 🤝 Contributing

### Development Setup

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make changes and test thoroughly
4. Commit with descriptive messages
5. Push and create a pull request

### Code Style

- Follow PEP 8 guidelines for Python code
- Use meaningful variable and function names
- Add docstrings to functions and classes
- Keep functions focused and single-purpose

### Testing

```bash
# Run tests (if test suite exists)
pytest tests/

# Check code style
flake8 .

# Type checking
mypy .
```

## 📝 License

Proprietary - All rights reserved

## 🆘 Support

For issues, questions, or contributions:
- Create an issue in the repository
- Contact the development team
- Check documentation in `/docs` (if available)

## 🔄 Version History

- **v1.0.0**: Initial release with IQR and DBSCAN algorithms
- **v1.1.0**: Added iteration management and manual override
- **v1.2.0**: Implemented item matching system
- **v1.3.0**: Bulk processing and combination analysis

## 🎓 Additional Resources

### Understanding IQR
- [Wikipedia: Interquartile Range](https://en.wikipedia.org/wiki/Interquartile_range)
- [Statistics: Outlier Detection](https://www.statisticshowto.com/statistics-basics/find-outliers/)

### Understanding DBSCAN
- [Scikit-learn DBSCAN Documentation](https://scikit-learn.org/stable/modules/generated/sklearn.cluster.DBSCAN.html)
- [DBSCAN Clustering Algorithm](https://en.wikipedia.org/wiki/DBSCAN)

### Flask Resources
- [Flask Documentation](https://flask.palletsprojects.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)

---

**Built with ❤️ for efficient product dimension analysis**
