FROM astrocrpublic.azurecr.io/runtime:3.1-14

# Python dependencies are auto-installed from requirements.txt by Astro
# System packages are auto-installed from packages.txt by Astro

# Environment variables are injected at runtime via .env
ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
