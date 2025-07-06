FROM python:3.12-slim

WORKDIR /app

COPY streamlit/requirments.txt ./streamlit/requirments.txt

RUN pip install -r streamlit/requirments.txt

COPY streamlit ./streamlit

EXPOSE 8501

CMD ["streamlit", "run", "streamlit/Main.py", "--server.port=8501", "--server.address=0.0.0.0"]
