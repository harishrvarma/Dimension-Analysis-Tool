# Product Dimension Analysis System

A Flask-based web application for analyzing product dimensions, detecting outliers using statistical methods and machine learning algorithms, and matching products across different systems.

## Overview

This system provides comprehensive tools for product data analysis, including dimension outlier detection, iterative refinement workflows, and intelligent product matching capabilities. It's designed to help identify anomalies in product dimensions and match products from different data sources.

## Key Features

### 1. Dimension Analysis & Outlier Detection
- **IQR (Interquartile Range)**: Statistical method for detecting outliers based on quartile ranges with configurable multipliers
- **DBSCAN**: Density-based clustering algorithm for identifying outliers and grouping similar products
- **Multi-iteration Analysis**: Refine analysis by running multiple iterations on normal or outlier subsets
- **Dynamic Filtering**: Filter by product groups, brands, categories, and types
- **Manual Override**: Mark products as outliers or normal regardless of algorithm results
- **Cluster Management**: Visualize and manage product clusters with outlier classification

### 2. Product Matching System
- **Multi-Algorithm Matching**: Support for TF-IDF, custom algorithms, and hybrid approaches
- **Configurable Attributes**: Match products based on SKU, name, price, URL, and custom attributes
- **Weighted Scoring**: Assign custom weights to different attributes for matching
- **Status Classification**: Automatic classification into Matched, Review, and Not Matched categories
- **Batch Processing**: Analyze products by brand/category combinations
- **Review Workflow**: Approve or reject matches with status tracking

### 3. Data Visualization
- **Interactive Charts**: Visualize dimension distributions and outlier patterns
- **Grid Views**: Browse products with filtering and sorting capabilities
- **Comparison Views**: Side-by-side comparison of system and competitor products
- **Cluster Visualization**: View DBSCAN clusters and outlier distributions

### 4. Database Management
- **CSV Import**: Import product data from CSV files
- **Migration Support**: Alembic-based database migrations
- **Aggregate Tracking**: Automatic calculation of aggregate outlier status across iterations
- **Audit Trail**: Track analysis history and iteration results

## Tech Stack

### Backend
- **Framework**: Flask (Python web framework)
- **Database**: SQLAlchemy ORM with Alembic migrations
- **Analysis Libraries**: 
  - Pandas (data manipulation)
  - NumPy (numerical operations)
  - Scikit-learn (machine learning algorithms)
- **Configuration**: Python-dotenv for environment management

### Frontend
- **Template Engine**: Jinja2
- **HTML/CSS**: Responsive web interface

## Project Structure

```
├── models/                      # Database models
│   ├── base/                    # Base model classes
│   ├── carton/                  # Carton product models
│   ├── core/                    # Core session management
│   ├── dimension/               # Dimension analysis models
│   │   ├── product.py           # Product model
│   │   ├── product_group.py     # Product group model
│   │   ├── product_iteration.py # Iteration tracking
│   │   └── product_iteration_item.py # Iteration items
│   └── matching/                # Product matching models
│       ├── matching_attribute.py
│       ├── matching_competitor_product.py
│       ├── matching_configuration_group.py
│       ├── matching_score_attributes.py
│       ├── matching_scores.py
│       └── matching_system_product.py
├── repositories/                # Data access layer
│   └── dimension/               # Dimension-specific repositories
│       ├── product_repository.py
│       ├── product_group_repository.py
│       ├── product_iteration_repository.py
│       └── product_iteration_item_repository.py
├── routes/                      # Flask route handlers
│   ├── database/                # Database operations routes
│   ├── dimension/               # Dimension analysis routes
│   │   ├── analyzer.py          # Analysis endpoints
│   │   ├── chart.py             # Chart data endpoints
│   │   ├── grid.py              # Grid view endpoints
│   │   └── outlier.py           # Outlier management endpoints
│   ├── item_match.py            # Product matching routes
│   └── matching_items.py        # Matching grid routes
├── services/                    # Business logic layer
│   ├── dimension/               # Dimension analysis services
│   │   ├── analyzer.py          # Core analysis logic
│   │   ├── chart.py             # Chart generation
│   │   ├── grid.py              # Grid data processing
│   │   └── outlier.py           # Outlier detection
│   └── item_match/              # Product matching services
│       ├── algorithms.py        # Matching algorithms
│       ├── attribute_service.py # Attribute management
│       ├── configuration_service.py # Configuration management
│       ├── matcher.py           # Main matching logic
│       └── score_service.py     # Score calculation
├── templates/                   # HTML templates
│   ├── database/                # Database UI templates
│   ├── dimension/               # Dimension analysis UI
│   ├── item_match/              # Product matching UI
│   └── matching_items/          # Matching grid UI
├── migrations/                  # Alembic database migrations
│   └── versions/                # Migration version files
├── logs/                        # Analysis logs and results
│   ├── analysis.log             # Application logs
│   ├── analysis_results.csv     # Analysis results
│   ├── normal_items.csv         # Normal products
│   └── outlier_items.csv        # Outlier products
├── app.py                       # Application entry point
├── config.py                    # Configuration loader
├── constants.py                 # Application constants
└── alembic.ini                  # Alembic configuration
```

## Installation

### Prerequisites
- Python 3.8 or higher
- pip (Python package manager)
- Database (SQLite/MySQL/PostgreSQL)

### Steps

1. **Clone the repository**
```bash
git clone <repository-url>
cd test
```

2. **Install dependencies**
```bash
pip install flask sqlalchemy alembic pandas numpy scikit-learn python-dotenv
```

3. **Configure environment variables**

Create a `.env` file in the root directory:
```env
APP_NAME=ProductAnalysisSystem
APP_HOST=localhost
APP_PORT=5000
SECRET_KEY=your-secret-key-here
DATABASE_URL=sqlite:///analysis.db
```

4. **Initialize database**
```bash
# Run migrations
alembic upgrade head
```

5. **Start the application**
```bash
python app.py
```

6. **Access the application**
```
http://localhost:5000
```

## Usage Guide

### Dimension Analysis

#### 1. Select Product Group
- Choose a product group from the dropdown
- System displays available brands, categories, and types

#### 2. Configure Analysis Parameters
- **Algorithms**: Select IQR, DBSCAN, or both
- **IQR Multipliers**: Adjust H, W, D multipliers (default: 1.5)
- **DBSCAN Parameters**: 
  - Epsilon (eps): Distance threshold for clustering
  - Min Samples: Minimum points to form a cluster

#### 3. Run Analysis
- Click "Analyze" to process products
- View results in grid or chart format
- Review outliers and normal products

#### 4. Iteration Workflow
- **Save Iteration**: Store current analysis results
- **New Iteration**: Analyze only normal or outlier products
- **Manual Override**: Mark clusters as outliers or remove outlier status
- **Reset**: Clear all iterations and start fresh

#### 5. View Results
- **Grid View**: Tabular display with filtering
- **Chart View**: Visual representation of dimensions
- **Outlier View**: Focused view of detected outliers

### Product Matching

#### 1. Configure Matching
- Select brands, categories, and types
- Choose matching algorithms (TF-IDF, Custom)
- Configure attribute weights
- Set threshold values for match classification

#### 2. Run Matching Analysis
- Click "Run Analysis" to process products
- System calculates similarity scores
- Products classified as Matched, Review, or Not Matched

#### 3. Review Matches
- View comparison details for each product
- See attribute-level scores
- Approve or reject matches
- Save top matches automatically

#### 4. Batch Processing
- Analyze all products by brand/category
- Track progress in real-time
- View completion status

### Data Import

1. Navigate to Database → Import CSV
2. Select CSV file with product data
3. Map columns to database fields
4. Import and validate data

## Algorithms Explained

### IQR (Interquartile Range)
- Calculates Q1 (25th percentile) and Q3 (75th percentile)
- IQR = Q3 - Q1
- Lower Bound = Q1 - (multiplier × IQR)
- Upper Bound = Q3 + (multiplier × IQR)
- Products outside bounds are marked as outliers
- Supports per-dimension multipliers for H, W, D

### DBSCAN (Density-Based Spatial Clustering)
- Groups products based on dimension similarity
- Parameters:
  - **eps**: Maximum distance between points in a cluster
  - **min_samples**: Minimum points to form a dense region
- Products not belonging to any cluster are outliers
- Automatically identifies noise points

### Product Matching Algorithms
- **TF-IDF**: Text similarity using term frequency-inverse document frequency
- **Custom**: Domain-specific matching for SKU, price, and URL
- **Hybrid**: Combines multiple algorithms with weighted scoring

## Database Schema

### Core Tables
- `dimension_product`: Product master data
- `dimension_product_group`: Product grouping
- `dimension_product_iteration`: Iteration tracking
- `dimension_product_iteration_item`: Per-product iteration results

### Matching Tables
- `matching_system_product`: Internal product catalog
- `matching_competitor_product`: Competitor product data
- `matching_scores`: Match scores and status
- `matching_score_attributes`: Attribute-level scores
- `matching_attribute`: Attribute definitions
- `matching_configuration_group`: Matching configurations

## Configuration

### Algorithm Constants
Defined in `constants.py`:
- `ALGO_IQR`: IQR algorithm identifier
- `ALGO_DBSCAN`: DBSCAN algorithm identifier

### Environment Variables
- `APP_NAME`: Application name
- `APP_HOST`: Server host (default: localhost)
- `APP_PORT`: Server port (default: 5000)
- `SECRET_KEY`: Flask secret key for sessions
- `DATABASE_URL`: Database connection string

## API Endpoints

### Dimension Analysis
- `GET /dimension/analyzer` - Analysis interface
- `POST /dimension/analyze` - Run analysis
- `POST /dimension/save-iteration` - Save iteration results
- `GET /dimension/grid` - Grid view
- `GET /dimension/chart` - Chart view
- `POST /dimension/outlier/set-cluster` - Mark cluster as outlier
- `POST /dimension/outlier/remove-cluster` - Remove outlier status

### Product Matching
- `GET /item-match` - Matching interface
- `POST /item-match/analyze` - Run matching analysis
- `GET /item-match/comparison/:id` - View comparison details
- `POST /item-match/save-match` - Save match result
- `POST /item-match/update-status` - Update review status

### Database Operations
- `GET /database/import` - Import interface
- `POST /database/import-csv` - Import CSV data

## Logging

Application logs are stored in `logs/analysis.log`:
- Analysis execution details
- Error messages and stack traces
- Database operations
- Iteration tracking

Result files:
- `analysis_results.csv`: Complete analysis results
- `normal_items.csv`: Products classified as normal
- `outlier_items.csv`: Products classified as outliers

## Best Practices

### Dimension Analysis
1. Start with default IQR multipliers (1.5)
2. Run initial analysis on all products
3. Review outliers and adjust parameters if needed
4. Use iterations to refine results
5. Save iterations before making manual changes

### Product Matching
1. Configure attribute weights based on importance
2. Start with higher thresholds and adjust down
3. Review "Review" status matches manually
4. Use batch processing for large datasets
5. Save configurations for reuse

### Performance
1. Filter by specific brands/categories for faster analysis
2. Use batch processing for large datasets
3. Save iterations to avoid reprocessing
4. Clear old iterations periodically

## Troubleshooting

### Common Issues

**Database Connection Errors**
- Verify DATABASE_URL in .env file
- Check database server is running
- Ensure migrations are up to date

**Analysis Fails**
- Check minimum data requirements (4+ products)
- Verify dimension data is not null
- Review logs/analysis.log for details

**Import Errors**
- Validate CSV format and column names
- Check for missing required fields
- Ensure data types match schema

## Contributing

1. Follow existing code structure
2. Add tests for new features
3. Update documentation
4. Follow PEP 8 style guidelines

## License

Proprietary - All rights reserved

## Support

For issues and questions:
- Check logs/analysis.log for error details
- Review database schema for data requirements
- Consult algorithm documentation for parameter tuning

---

**Version**: 1.0  
**Last Updated**: 2024  
**Python Version**: 3.8+
