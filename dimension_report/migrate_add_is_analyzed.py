"""
Migration script to add is_analyzed column to product table
Run this script once to update your existing database
"""

from sqlalchemy import text
from Models.database import engine

def add_is_analyzed_column():
    """Add is_analyzed column to product table if it doesn't exist"""
    
    with engine.connect() as conn:
        # Check if column exists
        result = conn.execute(text("""
            SELECT COUNT(*) as count
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = 'product'
            AND COLUMN_NAME = 'is_analyzed'
        """))
        
        exists = result.fetchone()[0] > 0
        
        if not exists:
            print("Adding is_analyzed column to product table...")
            conn.execute(text("""
                ALTER TABLE product
                ADD COLUMN is_analyzed VARCHAR(50) NULL
            """))
            conn.commit()
            print("✅ Column is_analyzed added successfully!")
        else:
            print("ℹ️ Column is_analyzed already exists")

if __name__ == "__main__":
    try:
        add_is_analyzed_column()
        print("\n✅ Migration completed successfully!")
    except Exception as e:
        print(f"\n❌ Migration failed: {str(e)}")
