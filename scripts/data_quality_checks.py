"""
Data Quality Checks for Hacker News Pipeline
Validates data integrity at each stage of the pipeline
"""
from datetime import datetime
import pandas as pd
import json
import os

class DataQualityException(Exception):
    """Custom exception for data quality failures"""
    pass

class DataQualityChecker:
    def __init__(self):
        self.checks_passed = []
        self.checks_failed = []
        
    def check_extraction_quality(self, data_path):
        """Validate extracted data quality"""
        print("=" * 50)
        print("RUNNING EXTRACTION QUALITY CHECKS")
        print("=" * 50)
        
        try:
            # Check 1: File exists
            if not os.path.exists(data_path):
                self._fail(f"Data file not found at {data_path}")
                return False
            self._pass(f"✓ Data file exists: {data_path}")
            
            # Load data
            with open(data_path, 'r') as f:
                stories = json.load(f)
            
            # Check 2: Minimum story count
            min_stories = 400
            story_count = len(stories)
            if story_count < min_stories:
                self._fail(f"Insufficient stories: {story_count} < {min_stories}")
            else:
                self._pass(f"✓ Story count OK: {story_count} stories")
            
            # Check 3: No duplicate IDs
            ids = [s.get('id') for s in stories]
            duplicates = len(ids) - len(set(ids))
            if duplicates > 0:
                self._fail(f"Found {duplicates} duplicate story IDs")
            else:
                self._pass(f"✓ No duplicate IDs found")
            
            # Check 4: Required fields present
            required_fields = ['id', 'title', 'by', 'score', 'time']
            missing_fields_count = 0
            for story in stories:
                for field in required_fields:
                    if field not in story or story[field] is None:
                        missing_fields_count += 1
                        break
            
            if missing_fields_count > len(stories) * 0.1:  # More than 10% missing
                self._fail(f"Too many stories missing required fields: {missing_fields_count}")
            else:
                self._pass(f"✓ Required fields present (only {missing_fields_count} incomplete)")
            
            # Check 5: Reasonable score values
            scores = [s.get('score', 0) for s in stories if s.get('score')]
            max_score = max(scores) if scores else 0
            if max_score > 10000:
                self._warn(f"Unusually high score detected: {max_score}")
            else:
                self._pass(f"✓ Score values reasonable (max: {max_score})")
            
            # Check 6: Data freshness (stories from last 7 days)
            timestamps = [s.get('time') for s in stories if s.get('time')]
            if timestamps:
                newest_story = max(timestamps)
                age_hours = (datetime.now().timestamp() - newest_story) / 3600
                if age_hours > 48:
                    self._warn(f"Newest story is {age_hours:.1f} hours old")
                else:
                    self._pass(f"✓ Data is fresh (newest story: {age_hours:.1f}h old)")
            
            return len(self.checks_failed) == 0
            
        except Exception as e:
            self._fail(f"Error during quality checks: {str(e)}")
            return False
    
    def check_transformation_quality(self, parquet_path):
        """Validate transformed data quality"""
        print("\n" + "=" * 50)
        print("RUNNING TRANSFORMATION QUALITY CHECKS")
        print("=" * 50)
        
        try:
            # Check 1: File exists
            if not os.path.exists(parquet_path):
                self._fail(f"Parquet file not found at {parquet_path}")
                return False
            self._pass(f"✓ Parquet file exists: {parquet_path}")
            
            # Load data
            df = pd.read_parquet(parquet_path)
            
            # Check 2: Row count reasonable
            if len(df) < 300:
                self._fail(f"Too few rows after transformation: {len(df)}")
            else:
                self._pass(f"✓ Row count OK: {len(df)} rows")
            
            # Check 3: No null values in critical columns
            critical_cols = ['id', 'title', 'posted_at']
            null_counts = df[critical_cols].isnull().sum()
            if null_counts.any():
                self._fail(f"Null values in critical columns: {null_counts[null_counts > 0].to_dict()}")
            else:
                self._pass(f"✓ No nulls in critical columns")
            
            # Check 4: Date parsing worked
            if df['posted_at'].dtype != 'datetime64[ns]':
                self._fail(f"posted_at not properly parsed as datetime: {df['posted_at'].dtype}")
            else:
                self._pass(f"✓ Timestamps properly parsed")
            
            # Check 5: Story type categorization
            story_types = df['story_type'].value_counts()
            if len(story_types) < 2:
                self._warn(f"Only one story type found: {story_types.to_dict()}")
            else:
                self._pass(f"✓ Story types categorized: {story_types.to_dict()}")
            
            # Check 6: Domain extraction
            domain_null_pct = df['domain'].isnull().sum() / len(df) * 100
            if domain_null_pct > 30:
                self._warn(f"High percentage of missing domains: {domain_null_pct:.1f}%")
            else:
                self._pass(f"✓ Domain extraction OK ({domain_null_pct:.1f}% missing)")
            
            return len(self.checks_failed) == 0
            
        except Exception as e:
            self._fail(f"Error during transformation quality checks: {str(e)}")
            return False
    
    def check_load_quality(self, engine, expected_min_rows=300):
        """Validate data in PostgreSQL"""
        print("\n" + "=" * 50)
        print("RUNNING LOAD QUALITY CHECKS")
        print("=" * 50)
        
        try:
            # Check 1: Table exists and has data
            query = "SELECT COUNT(*) as cnt FROM stories"
            result = pd.read_sql(query, engine)
            row_count = result.iloc[0]['cnt']
            
            if row_count < expected_min_rows:
                self._fail(f"Insufficient rows in database: {row_count} < {expected_min_rows}")
            else:
                self._pass(f"✓ Database row count OK: {row_count} rows")
            
            # Check 2: No duplicate IDs
            dup_query = """
            SELECT id, COUNT(*) as cnt 
            FROM stories 
            GROUP BY id 
            HAVING COUNT(*) > 1
            """
            duplicates = pd.read_sql(dup_query, engine)
            if len(duplicates) > 0:
                self._fail(f"Found {len(duplicates)} duplicate IDs in database")
            else:
                self._pass(f"✓ No duplicate IDs in database")
            
            # Check 3: Data distribution over time
            date_query = """
            SELECT DATE(posted_at) as date, COUNT(*) as cnt
            FROM stories
            GROUP BY DATE(posted_at)
            ORDER BY date DESC
            LIMIT 7
            """
            date_dist = pd.read_sql(date_query, engine)
            self._pass(f"✓ Data distribution:\n{date_dist.to_string(index=False)}")
            
            return len(self.checks_failed) == 0
            
        except Exception as e:
            self._fail(f"Error during load quality checks: {str(e)}")
            return False
    
    def _pass(self, message):
        """Log a passed check"""
        print(f"  {message}")
        self.checks_passed.append(message)
    
    def _fail(self, message):
        """Log a failed check"""
        print(f"  ✗ FAILED: {message}")
        self.checks_failed.append(message)
    
    def _warn(self, message):
        """Log a warning"""
        print(f"  ⚠ WARNING: {message}")
    
    def print_summary(self):
        """Print summary of all checks"""
        print("\n" + "=" * 50)
        print("DATA QUALITY CHECK SUMMARY")
        print("=" * 50)
        print(f"✓ Passed: {len(self.checks_passed)}")
        print(f"✗ Failed: {len(self.checks_failed)}")
        
        if self.checks_failed:
            print("\nFailed checks:")
            for failure in self.checks_failed:
                print(f"  - {failure}")
            raise DataQualityException(f"{len(self.checks_failed)} quality checks failed")
        else:
            print("\n✓ All quality checks passed!")

# Convenience function for use in DAG
def run_extraction_checks(data_path):
    checker = DataQualityChecker()
    checker.check_extraction_quality(data_path)
    checker.print_summary()

def run_transformation_checks(parquet_path):
    checker = DataQualityChecker()
    checker.check_transformation_quality(parquet_path)
    checker.print_summary()

def run_load_checks(engine):
    checker = DataQualityChecker()
    checker.check_load_quality(engine)
    checker.print_summary()