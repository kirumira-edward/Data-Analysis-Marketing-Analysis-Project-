import pandas as pd
from sqlalchemy import create_engine, exc
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Download the VADER lexicon
nltk.download('vader_lexicon')

def fetch_data_from_sql():
    try:
        # SQLAlchemy connection string with proper escaping
        connection_string = (
            r"mssql+pyodbc://DESKTOP-NAC8998\SQLEXPRESS/PortfolioProject_MarketingAnalytics?"
            r"driver=ODBC+Driver+17+for+SQL+Server&"
            r"Trusted_Connection=yes"
        )
        
        engine = create_engine(connection_string)
        
        # Query with explicit schema qualification
        query = """
        SELECT ReviewID, CustomerID, ProductID, 
               ReviewDate, Rating, ReviewText 
        FROM dbo.customer_reviews  -- Using dbo schema
        """
        
        return pd.read_sql(query, engine)
    
    except exc.SQLAlchemyError as e:
        print(f"Database error: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error
    except Exception as e:
        print(f"General error: {str(e)}")
        return pd.DataFrame()

# Fetch data
customer_reviews_df = fetch_data_from_sql()

# Check if data was loaded successfully
if customer_reviews_df.empty:
    print("No data loaded. Check database connection and table existence.")
    exit()

# Sentiment Analysis Setup
sia = SentimentIntensityAnalyzer()

def calculate_sentiment(review):
    return sia.polarity_scores(str(review))['compound']  # Handle potential NaN

def categorize_sentiment(score, rating):
    # Your existing logic with float conversion
    score = float(score)
    rating = int(rating)
    
    if score > 0.05:
        if rating >= 4:
            return 'Positive'
        elif rating == 3:
            return 'Mixed Positive'
        else:
            return 'Mixed Negative'
    elif score < -0.05:
        if rating <= 2:
            return 'Negative'
        elif rating == 3:
            return 'Mixed Negative'
        else:
            return 'Mixed Positive'
    else:
        if rating >= 4:
            return 'Positive'
        elif rating <= 2:
            return 'Negative'
        else:
            return 'Neutral'

def sentiment_bucket(score):
    score = float(score)
    if score >= 0.5:
        return '0.5 to 1.0'
    elif 0.0 <= score < 0.5:
        return '0.0 to 0.49'
    elif -0.5 <= score < 0.0:
        return '-0.49 to 0.0'
    else:
        return '-1.0 to -0.5'

# Apply sentiment analysis
customer_reviews_df['SentimentScore'] = customer_reviews_df['ReviewText'].apply(calculate_sentiment)
customer_reviews_df['SentimentCategory'] = customer_reviews_df.apply(
    lambda row: categorize_sentiment(row['SentimentScore'], row['Rating']), axis=1)
customer_reviews_df['SentimentBucket'] = customer_reviews_df['SentimentScore'].apply(sentiment_bucket)

# Save and display results
try:
    customer_reviews_df.to_csv('fact_customer_reviews_with_sentiment.csv', index=False)
    print("Success! First 5 rows:")
    print(customer_reviews_df.head())
except Exception as e:
    print(f"Error saving results: {str(e)}")