FROM astrocrpublic.azurecr.io/runtime:3.1-14

# Python dependencies (system packages are auto-installed from packages.txt by Astro)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Environment variables are injected at runtime via .env
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
