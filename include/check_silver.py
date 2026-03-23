from google.cloud import bigquery

def run():
    client = bigquery.Client(project='yelp-490821')
    
    tables = ['reviews', 'businesses', 'users']
    for t in tables:
        print(f"\n--- Checking Silver Table: {t} ---")
        try:
            query = f"SELECT COUNT(*) as cnt FROM `yelp-490821.silver.{t}`"
            job = client.query(query)
            res = list(job.result())
            print(f"✅ Total Rows in {t}: {res[0].cnt}")
            
            # Print a sample row
            if res[0].cnt > 0:
                sample_query = f"SELECT * FROM `yelp-490821.silver.{t}` LIMIT 1"
                sample_job = client.query(sample_query)
                sample_res = list(sample_job.result())
                sample_dict = dict(sample_res[0])
                
                # Truncate long strings for clean output
                for k, v in sample_dict.items():
                    if isinstance(v, str) and len(v) > 100:
                        sample_dict[k] = v[:100] + "..."
                
                print(f"🔍 Sample Row:\n{sample_dict}")
        except Exception as e:
            print(f"❌ Failed to query {t}: {e}")

if __name__ == '__main__':
    run()
