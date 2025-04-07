from transformers import pipeline
sentiment_analyzer = pipeline("text-classification", model="ProsusAI/finbert")

def analyze_article(text):
    sentiment = sentiment_analyzer(text)[0]  # e.g., {"label": "positive", "score": 0.92}
    # entities = nlp(text).ents  # Extract companies (e.g., "AAPL")
    return sentiment#, entities