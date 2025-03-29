# Stock Agentic AI

# Install Dependencies
```shell
pip install -r requirements.txt
```

# Run
```shell
streamlit run ui/app.py
```

# Prepare data
```shell
docker-compose -f docker/docker-compose-amd64.yml up -d 
python process_stock_data.py
python process_news_data.py
```